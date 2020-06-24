// Package xrootd provides a filesystem interface using github.com/go-hep/hep/tree/master/xrootd

package xrootd

import(
	  "context"
	  "time"
	  "io"
	  "os"
	  "path"
	  "path/filepath"
	  "fmt"
	  "sync"
		gohash "hash"

		//xrootd
	  "go-hep.org/x/hep/xrootd"
	  "go-hep.org/x/hep/xrootd/xrdio"
	  "go-hep.org/x/hep/xrootd/xrdfs"
		"go-hep.org/x/hep/xrootd/xrdproto/query"

		//hash adler32
		"hash/adler32"

		//rclone
	  "github.com/pkg/errors"
	  "github.com/rclone/rclone/fs"
	  //"github.com/rclone/rclone/fs/config"
	  "github.com/rclone/rclone/fs/config/configmap"
	  "github.com/rclone/rclone/fs/config/configstruct"
	  "github.com/rclone/rclone/fs/hash"
	  "github.com/rclone/rclone/lib/readers"


)

// Constants
const (
	defaultCopyBufferKb = 1024
)


// Globals
var (
	// Adler32HashType is the hash.Type for XrootD
	Adler32HashType hash.Type
)

// Register with Fs
func init(){
	Adler32HashType = hash.RegisterHash("Adler32Hash", 8, func() gohash.Hash { return adler32.New() })
	fsi :=&fs.RegInfo{
		Name:        "xrootd",
		Description: "xrootd-client",
		NewFs:       NewFs,

		Options: []fs.Option{{
			Name: "servername",
			Help: "xrootd servername, leave blank to use default",
			Default:"localhost",
		}, {
			Name: "port",
			Help: "Xrootd port, leave blank to use default",
			Default:"1094",
		}, {
			Name: "path_to_file",
			Help: "Xrootd root path, example (/tmp)",
			Default:"/",
		}, {
			Name: "size_copy_buffer_kb",
			Help: "Choose the size of the transfer buffer, leave blank to use default (1 MB by default)",
			Default: defaultCopyBufferKb,
			Advanced: true,
		}},
	}
	fs.Register(fsi)
}





type Options struct {
	Servername        string `config:"servername"`
	Port              string `config:"port"`
	Path_to_file      string `config:"path_to_file"`
	SizeCopyBufferKb  	int64  `size_copy_buffer_kb`
	//Pass            string `config:"pass"`
	//AskPassword      bool   `config:"ask_password"`
}



type Fs struct {
	name            string                // name of this remote
	root            string                // the path we are working on
	opt             Options               // parsed options
	//m             configmap.Mapper // config
	url             string
	features        *fs.Features          // optional features
	objectHashesMu  sync.Mutex // global lock for Object.hashes
}


type Object struct {
	fs            *Fs           // what this object is part of
	remote        string       // The remote path
	size          int64       // size of the object
	modTime       time.Time   // modification time of the object if known
	mode          os.FileMode
	hash    string    // content_hash of the object
}


// Open a new connection to the xrootd server.
func (f *Fs) xrdremote(name string, ctx context.Context) (client *xrootd.Client, path string, err error) {
	url, err := xrdio.Parse(name)
	if err != nil {
		return nil, "", fmt.Errorf("could not parse %q: %w", name, err)
	}
	path = url.Path
	client, err = xrootd.NewClient(ctx, url.Addr, url.User)
	return client, path, err
}



// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
	fs.Debugf(name,"Using the newfs function")
	ctx := context.Background()

	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	url := "root://" + opt.Servername + ":" + opt.Port + "/" + opt.Path_to_file +"/" + root

	fs.Debugf(name,"Newfs Copy buffer size in KB: %v, path: %v", opt.SizeCopyBufferKb,url)

	f := &Fs{
		name:      name,
		root:      root,
		opt:       *opt,
		//m:         m,
		url:       url,
	}

	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(f)

	cli,path,err := f.xrdremote(url, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "NewFs")
	}
	defer cli.Close()


	if root != "" {
		// Check to see if the root actually an existing file
		remote := filepath.Base(path)
		f.root = filepath.Dir(path)
		if f.root == "." {
			f.root = ""
		}
		_, err := f.NewObject(ctx, remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound || errors.Cause(err) == fs.ErrorNotAFile {
				// File doesn't exist so return old f
				f.root = path
				return f, nil
			}
			return nil, err
		}
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
  return f, nil
}


// Name returns the configured name of the file system
func (f *Fs) Name() string {
	return f.name
}


//Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(Adler32HashType)
}


// NewObject finds the Object at remote.  If it can't be found
//
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(f, "Using the fs NewObject function with remote: %s", remote)
	o := &Object{
		fs:     f,
		remote: remote,
	}
	err := o.stat(ctx)
	if err != nil {
		return nil, err
	}

	return o, nil
}



// setMetadata sets the file info from the os.FileInfo passed in
func (o *Object) setMetadata(info os.FileInfo) {
	fs.Debugf(o,"Using the object setMetadata function with FileInfo: %v", info)
	if o.size != info.Size() {
		fs.Debugf(o,"setMetadata modified size: %v -> %v",o.size, info.Size())
		o.size = info.Size()
	}
	if !o.modTime.Equal(info.ModTime()) {
		fs.Debugf(o,"setMetadata modified ModTime: %v -> %v",o.modTime, info.ModTime())
		o.modTime = info.ModTime()
	}
	if o.mode != info.Mode() {
		fs.Debugf(o,"setMetadata modified Mode: %v -> %v",o.mode, info.Mode())
		o.mode = info.Mode()
	}
		fs.Debugf(o,"setMetadata size: %v , modTime: %v, mode: %v", o.size, o.modTime, o.mode)
}


//Continuation of the List function
func (f *Fs) display(ctx context.Context, fsx xrdfs.FileSystem, root string, info os.FileInfo, dir string ) (entries fs.DirEntries, err error) {
	fs.Debugf(f,"Using the fs display function with xrdfs.FileSystem: %v, root: %v ,info: %v and dir= %v", fsx, root, info, dir)

	dirt := path.Join(root, info.Name())
	ents, err := fsx.Dirlist(ctx, dirt)

	if err != nil {
		return nil,fmt.Errorf("could not list dir %q: %w", dirt, err)
	}


	for _, info := range ents {
		remote := path.Join(dir, info.Name())
		if info.IsDir() {
			d := fs.NewDir(remote, info.ModTime())
			entries = append(entries, d)
		} else {
			o := &Object{
				fs:     f,
				remote: remote,
			}
			o.setMetadata(info)
			entries = append(entries, o)
		}
	}

	return entries,nil
}


// List the objects and directories in dir into entries. The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fs.Debugf(f,"Using the fs list function with directory: %s", dir)

	xrddir := f.url+ "/"+ dir

	client,path,err :=f.xrdremote(xrddir,ctx)
	if path == "" {
		path = "."
	}

	if err != nil{
		return nil, fmt.Errorf("could not stat %q: %w", path, err)
	}
	defer client.Close()

	fsx := client.FS()
	fi,err := fsx.Stat(ctx,path)

	if err != nil {
		fs.Debugf(f,"List :dir not found with path= %v ", path)
		return nil, fs.ErrorDirNotFound
	}
	entries,err = f.display(ctx, fsx, path, fi, dir )
	if  err != nil {
		return entries,err
	}

	err = client.Close();
	if  err != nil {
		return entries,err
	}
	return entries,nil
}




// Mkdir creates the directory if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	fs.Debugf(f,"Using the fs Mkdir function with directory: %s", dir)

	xrddir := f.url+ "/"+ dir
	client,path,err :=f.xrdremote(xrddir,ctx)
	if err != nil{
		return err
	}
	defer client.Close()

	err = client.FS().MkdirAll(ctx,path,755)
	if err != nil {
    fs.Debugf(f,"failed Mkdir: %v", path)
		return err
	}
  fs.Debugf(f,"Mkdir: %v", path)

	err = client.Close();
	if  err != nil {
		return err
	}
	return nil
}


// Rmdir deletes the root folder
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	fs.Debugf(f,"Using the fs Rmdir function with directory: %s", dir)

	// Check to see if directory is empty
	entries, err := f.List(ctx, dir)
	if err != nil {
		return errors.Wrap(err, "Rmdir")
	}
	if len(entries) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	// Remove the directory
	xrddir := f.url+ "/"+ dir

	client,path,err :=f.xrdremote(xrddir,ctx)
	if err != nil{
		return err
	}
	defer client.Close()

	err = client.FS().RemoveDir(ctx, path)
	if  err != nil {
    fs.Debugf(f,"Failed Remove directory: %v", path)
		return err
	}
  fs.Debugf(f,"Remove directory: %v", path)
	err = client.Close();
	if  err != nil {
		return err
	}

	return nil
}



// Purge deletes all the files and directories
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context) error {
	fs.Debugf(f,"Using the fs Purge function")

	client,path,err := f.xrdremote(f.root,ctx)
	if err != nil{
		return err
	}
	defer client.Close()

	err = client.FS().RemoveAll(ctx, path);
	if  err != nil {
    fs.Debugf(f,"Failed Remove All: %v", path)
		return err
	}
  fs.Debugf(f,"Remove All: %v", path)

	err = client.Close();
	if  err != nil {
		return err
	}
	return nil
}


// Move renames a remote xrootd file object
//
// It returns the destination Object and a possible error
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	fs.Debugf(f,"Using the fs Move function with src: %v and remote: %s", src,remote)

	srcObj, ok := src.(*Object)

	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	xrddst := f.url+ "/"+ remote

	//Source path
	client,pathsrc,err := f.xrdremote(srcObj.path(),ctx)
	if err != nil{
		return nil, errors.Wrap(err, "Move")
	}
	defer client.Close()

	//Destination path
	client,pathdst,err :=f.xrdremote(xrddst,ctx)
	if err != nil{
		return nil, errors.Wrap(err, "Move")
	}
	defer client.Close()

	err = client.FS().Rename(ctx, pathsrc, pathdst);
	if err != nil {
    fs.Debugf(f,"failed Move: %v -> %v", pathsrc, pathdst)
		return nil, errors.Wrap(err, "Move Rename failed")
	}


	dstObj, err := f.NewObject(ctx, remote)
	if err != nil {
		return nil, errors.Wrap(err, "Move NewObject failed")
	}
  fs.Debugf(f,"Move: %v -> %v", pathsrc, pathdst)

	err = client.Close();
	if  err != nil {
		return dstObj,err
	}

	return dstObj, nil
}


// dirExists returns true,nil if the directory exists, false, nil if
// it doesn't or false, err
func (f *Fs) dirExists(ctx context.Context, dir string) (bool, error) {
	client,path,err :=f.xrdremote(dir,ctx)

	if err != nil{
		return false, fmt.Errorf("could not stat %q: %w", path, err)
	}
	defer client.Close()

	fsx := client.FS()
	info,err := fsx.Stat(ctx,path)
	if err!=nil{
		if os.IsNotExist(err){
			return false, nil
		}
		return false, errors.Wrap(err, "dirExists stat failed")
	}
	if !info.IsDir() {
		return false, fs.ErrorIsFile
	}
	return true, nil
}


// DirMove moves src, srcRemote to this remote at dstRemote
// using server side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	fs.Debugf(f,"Using the fs DirMove function with src: %v, srcRemote: %s and dstRemote: %s", src, srcRemote, dstRemote)

	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	srcPath := path.Join(srcFs.root, srcRemote)
	dstPath := f.url+ "/"+ dstRemote

	client,path,err :=f.xrdremote(dstPath,ctx)
	if err != nil{
		return errors.Wrap(err, "dirMove not open client")
	}
	defer client.Close()

	// Check if destination exists
	ok, err = f.dirExists(ctx,path)
	if ok {
		return fs.ErrorDirExists
	}

  // Make sure the parent directory exists
  err = client.FS().MkdirAll(ctx,filepath.Dir(path),755)
	if err != nil {
    fs.Debugf(f,"Failed Mkdir: %v", filepath.Dir(path))
		return errors.Wrap(err, "DirMove mkParentDir dst failed")
	}
  fs.Debugf(f,"Mkdir: %v", filepath.Dir(path))

	err = client.FS().Rename(ctx, srcPath , path);
	if err != nil {
    fs.Debugf(f,"Failed directory Move: %v -> %v",srcPath, path)
		return errors.Wrapf(err, "DirMove Rename(%q,%q) failed", srcPath, dstPath)
	}
  fs.Debugf(f,"Directory Move: %v->%v",srcPath, path)

	err = client.Close();
	if  err != nil {
		return err
	}

	return  nil
}

// Precision of the file system
func (f *Fs) Precision() time.Duration {
	return time.Second
}


// Put data from <in> into a new remote xrootd file object described by <src.Remote()> and <src.ModTime(ctx)>
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Debugf(f,"Using the fs Put function with in: %v and srcRemote: %v", in, src)

	o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	err := o.Update(ctx, in, src, options...)
	if err != nil {
		return nil, err
	}
	return o, nil
}


// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}


// String converts this Fs to a string (String returns the URL for the filesystem)
func (f *Fs) String() string {
	return f.url
}



// statRemote stats the file or directory at the remote given
func (f *Fs) stat(ctx context.Context, remote string) (info os.FileInfo, err error) {
	fs.Debugf(f,"Using the fs stat function with remote: %s ", remote)

	xrddir := f.url
	if filepath.Base(f.url) != remote{
	  xrddir = f.url+ "/"+ remote
	}

	client,path,err :=f.xrdremote(xrddir,ctx)
	if err != nil{
		return nil, fmt.Errorf("could not stat %q: %w", path, err)
	}
	defer client.Close()

	fsx := client.FS()
	info,err = fsx.Stat(ctx,path)
	if  err != nil {
		return info,err
	}
	fs.Debugf(f,"Stat : %v", info)
	err = client.Close();
	if  err != nil {
		return info,err
	}

	return info, nil
}


// stat updates the info in the Object
func (o *Object) stat(ctx context.Context) error {
	fs.Debugf(o,"Using the Object stat function")
	info, err := o.fs.stat(ctx, o.remote)

	if err != nil {

		//if os.IsNotExist(err) {
			//return fs.ErrorObjectNotFound
   		//}
		//return errors.Wrap(err, "stat failed")
		return fs.ErrorObjectNotFound
	}
	if info.IsDir() {
		return errors.Wrapf(fs.ErrorNotAFile, "%q", o.remote)
	}
	o.setMetadata(info)
	return nil
}




// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}


// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}



// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}



// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}


// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}



// Hash returns adler32 checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	fs.Debugf(o,"Using hash function with hash.Type= %v",t)
	if t != Adler32HashType {
		return "", hash.ErrUnsupported
	}

	// Retrieve the checksum of the file by asking the xrootd server
	client,path,err := o.fs.xrdremote(o.path(),ctx)
	if err != nil {
		return "", err
	}

	file, err := client.FS().Open(ctx, path, xrdfs.OpenModeOwnerRead, xrdfs.OpenOptionsOpenRead)
	if err != nil {
		return "", err
	}
	defer file.Close(ctx)

	fs.Debugf(o,"Hash:path= %v",path)

  var (
    resp query.Response
    req = query.Request{
      Query: query.Checksum,
      Args:  []byte(path),
    }
  )

  _, err = client.Send(ctx, &resp, &req)
  if err != nil {
		fs.Debugf(o,"Checksum request error", err)
    return "",err
  }
	o.hash = string(resp.Data[8:16])  //Because resp.Data = "adler32 95ec3712\x00"
	fs.Debugf(o,"Hash: o.Hash= %v && Data=%v",o.hash,string(resp.Data))
	return o.hash, nil
}



// path returns the native path of the object
func (o *Object) path() string {
	if filepath.Base(o.fs.url) != o.remote{
		return o.fs.url+ "/"+ o.remote
	}
	return o.fs.url
}




// object that is read
type xrdOpenFile struct {
	o    *Object           // object that is open
	xrdfile *xrdio.File    // file object reference
	bytes int64
	eof   bool
}

func newObjectReader(o *Object, xrdfile *xrdio.File) *xrdOpenFile {
  fs.Debugf(xrdfile,"Using newObjectReader function")
	file := &xrdOpenFile{
		o:    o,
		xrdfile:   xrdfile,
		bytes: 0,
		eof: false,
	}
	return file
}

// Read bytes from the object - see io.Reader
func (file *xrdOpenFile) Read(p []byte) (n int, err error) {
  fs.Debugf(file,"Using Read function %v",file.o)
	n, err = file.xrdfile.Read(p)
	file.bytes += int64(n)
	if err == io.EOF {
		file.eof = true
	}
	return n,err
}



// Close the object
func (file *xrdOpenFile) Close() (err error) {
  fs.Debugf(file,"Using Close function")

	if file.eof {
	  fs.Debugf(file,"end of file reached")
	}else{
		fs.Debugf(file,"end of file isn't reached")
	}
	err = file.xrdfile.Close()

	// Check to see we read the correct number of bytes
	if file.o.Size() != file.bytes {
		return errors.Errorf("object corrupted on transfer - length mismatch (want %d got %d)", file.o.Size(), file.bytes)
	}

	return err
}


// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	fs.Debugf(o,"Using the object Open function")
	var offset, limit int64 = 0, -1

	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset = x.Offset
		case *fs.RangeOption:
			offset, limit = x.Decode(o.Size())
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}

	xrdfile, err := xrdio.Open(o.path())
	if err!= nil{
    fs.Debugf(o,"failed Open file: %v", o.path())
		return nil, errors.Wrap(err, "Open failed")
	}
  fs.Debugf(o,"Open file: %v", o.path())

	if offset > 0 {
		off, err := xrdfile.Seek(offset, io.SeekStart)
		if err != nil || off != offset {
			return nil, errors.Wrap(err, "Open Seek failed")
		}
	}

	in = readers.NewLimitedReadCloser(newObjectReader(o , xrdfile), limit)
	return in, nil
}




// SetModTime sets the modification and access time to the specified time
//
// it also updates the info field
func (o *Object) SetModTime(ctx context.Context,modTime time.Time) error {
	fs.Debugf(o,"Using the object SetModTime function with modTime: %v", modTime)

	/*client,path,err :=o.fs.xrdremote(o.path(),ctx)
	if err != nil{
		return errors.Wrap(err, "SetModTime")
	}
	defer client.Close()*/

	o.modTime = modTime
/*	err = os.Chtimes(path, modTime, modTime)
	if err != nil {
		return errors.Wrap(err, "SetModTime failed")
	}*/


	err := o.stat(ctx)
	if err != nil {
		return errors.Wrap(err, "SetModTime stat failed")
	}
	return nil
}


// Storable returns a boolean showing if this object is storable
func (o *Object) Storable() bool {
	return o.mode.IsRegular()
}


// Update the object from in with modTime and size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	fs.Debugf(o,"Using the object Update function with in: %v", in)
	o.hash = ""

	client,path,err :=o.fs.xrdremote(o.path(),ctx)
	if err != nil{
		fs.Debugf(src, "Failed to open client", err)
		return err
	}
	defer client.Close()

/*	err = client.FS().MkdirAll(ctx, filepath.Dir(path), 755)*/

	file,err := client.FS().Open(ctx, path, 0755, xrdfs.OpenOptionsNew|xrdfs.OpenOptionsMkPath)
	if err != nil {
		fs.Debugf(src, "Failed to open new file", err)
		//the file may already exist, attempt to open it
		file,err = client.FS().Open(ctx, path, 0755, xrdfs.OpenOptionsOpenUpdate|xrdfs.OpenOptionsMkPath|xrdfs.OpenOptionsDelete)
		if err != nil {
			fs.Debugf(src, "Failed to open an existing file", err)
			return err
		}
	}

	// remove the file if upload failed
	remove := func() {
		client,path,removeErr :=o.fs.xrdremote(o.path(),ctx)
		if removeErr != nil{
			fs.Debugf(src, "Failed to open client", removeErr)
			return
		}
		defer client.Close()
		removeErr = client.FS().RemoveFile(ctx, path);

			if removeErr != nil {
				fs.Debugf(src, "Failed to remove: %v", removeErr)
			} else {
				fs.Debugf(src, "Removed after failed upload: %v", err)
			}
			removeErr = client.Close();
			if  removeErr != nil {
				fs.Debugf(src, "Failed to close client ", removeErr)
				return
			}
		}

		checksum := adler32.New()
		var bufsize int64 =o.fs.opt.SizeCopyBufferKb * 1024
		data := make([]byte, bufsize)
		var  err_read error
		var  index int64 = 0
		var  n int
		var  turn int64 = 0  //number of turns

		for {
			n, err_read = in.Read(data)
			if ((err_read != nil) && (err_read != io.EOF)) {
				err = err_read
				return errors.Wrap(err, "update: could not read data")
			}

			_,err = file.WriteAt(data[:n], index)

			if err != nil {
				remove()
				return errors.Wrap(err, "update: could not copy to output file")
			}

			checksum.Write(data[:n])  //update checksum data

			index += int64(n)
			turn += 1
			if err_read == io.EOF {
				// source has been read until End Of File
				break
			}

		}

		o.hash = fmt.Sprintf("%x", checksum.Sum32())  //checksum is int32
		fs.Debugf(src,"Update: Checksum %x",checksum.Sum32())

		fs.Debugf(src, "Update: avg buff size= %d", index / turn )
		fs.Debugf(src, "Update: src size %v vs copy size %v", src.Size(), index)

		err = file.Close(ctx)
		if err != nil {
			remove()
			return errors.Wrap(err,"could not close output file")
		}

		err = o.SetModTime(ctx, src.ModTime(ctx))
		if err != nil {
			return errors.Wrap(err, "Update: SetModTime failed")
		}

		return nil
}


// Remove a remote xrootd file object
func (o *Object) Remove(ctx context.Context) error {
	fs.Debugf(o,"Using the object Remove function")

	client,path,err :=o.fs.xrdremote(o.path(),ctx)
	if err != nil{
		return err
	}
	defer client.Close()

	err = client.FS().RemoveFile(ctx, path);
	if  err != nil {
    fs.Debugf(o,"Failed remove File: %v",path)
		return err
	}
  fs.Debugf(o,"Remove File: %v",path)

	err = client.Close();
	if  err != nil {
		return err
	}
	return nil
}



// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.Mover       = &Fs{}
	_ fs.DirMover    = &Fs{}
	_ fs.Object      = &Object{}
)

