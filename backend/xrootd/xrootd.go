// Package xrootd provides a filesystem interface using github.com/go-hep/hep/tree/master/xrootd

//a retirer
//commentaire

package xrootd

import(
  "context"
  "time"
	"io"
  "os"
  "path"
  "path/filepath"
//  "strings"
  "fmt"



  "go-hep.org/x/hep/xrootd"
  "go-hep.org/x/hep/xrootd/xrdio"
  "go-hep.org/x/hep/xrootd/xrdfs"


  "github.com/pkg/errors"
  "github.com/rclone/rclone/fs"
//	"github.com/rclone/rclone/fs/config"
  "github.com/rclone/rclone/fs/config/configmap"
  "github.com/rclone/rclone/fs/config/configstruct"
  "github.com/rclone/rclone/fs/hash"
  "github.com/rclone/rclone/lib/readers"
)

// Constants
const (
  titre_fonction = false
)


// Globals
var (
//  currentUser = readCurrentUser()
)

// Register with Fs
func init(){
  fsi :=&fs.RegInfo{
    Name:        "xrootd",
    Description: "xrootd-client",
    NewFs:       NewFs,

    Options: []fs.Option{{
    Name:     "path_xroot",
    Help:     "xrootd host to connect to (default 'root' )",
    Required: true,

    }, {
  			Name: "user",
  			Help: "xrootd username (default 'localhost') ",// leave blank for current username, " + currentUser,
      //  Required: true,
      }, {
    		Name: "port",
    		Help: "Xrootd port, leave blank to use default (1094)",
      }, {
        Name: "path_to_file",
        Help: "Xrootd path path-to-file, example (/tmp) and default '/'",
      }},
    }
    fs.Register(fsi)
}





type Options struct {

	Host              string `config:"host"`
	User              string `config:"user"`
	Port              string `config:"port"`
  Path_xroot        string `config:"path_xroot"`
  Path_to_file      string `config:"path_to_file"`
  //Pass              string `config:"pass"`
  //AskPassword       bool   `config:"ask_password"`
}



type Fs struct {
  name         string                // name of this remote
	root         string                // the path we are working on
	opt          Options               // parsed options
  url          string
  features     *fs.Features          // optional features
//	srv          *rest.Client          // the connection to the one drive server
}


type Object struct {
	fs            *Fs           // what this object is part of
	remote        string       // The remote path
  hasMetaData   bool      // whether info below has been set
	size          int64       // size of the object
	modTime       time.Time   // modification time of the object if known
  mode          os.FileMode
  sha1          string    // SHA-1 of the object content

	//mode    os.FileMode // mode bits from the file
  //	md5sum  *string     // Cached MD5 checksum
  //	sha1sum *string     // Cached SHA1 checksum
  //id          string    // ID of the object
}



/*
 readCurrentUser finds the current user name or "" if not found
func readCurrentUser() (userName string) {
	usr, err := user.Current()
	if err == nil {
		return usr.Username
	}
	 //Fall back to reading $USER then $LOGNAME
	userName = os.Getenv("USER")
	if userName != "" {
		return userName
	}
	return os.Getenv("LOGNAME")
}
*/


func (f *Fs) xrdremote(name string, ctx context.Context) (client *xrootd.Client, path string, err error) {
	url, err := xrdio.Parse(name)
	if err != nil {
		return nil, "", fmt.Errorf("could not parse %q: %w", name, err)
	}
	path = url.Path
	client, err = xrootd.NewClient(ctx, url.Addr, url.User)
	return client, path, err
}



func (f *Fs) connectxrootclient(scr string, ctx context.Context) (fi os.FileInfo,path string ,fsx xrdfs.FileSystem, err error){
  url, err := xrdio.Parse(scr)
  if err!= nil{

    return nil, "", nil, errors.Wrap(err, "could not parse "+ scr)
  }
  client, err := xrootd.NewClient(ctx, url.Addr, url.User)  //client *xrootd.Client


	if err != nil {
    return nil, "", nil, errors.Wrap(err, "could not create client ")
	}
  defer client.Close()

  fsx = client.FS()

	fi, err = fsx.Stat(ctx, url.Path)
	// TODO fi.Name() here is an empty string (see handling in format() below)
	if err != nil {
    return nil, "",nil, errors.Wrap(err," could not stat" + url.Path )
	}
  return fi ,url.Path, fsx , nil
}



// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction newfs \n")
  }

  ctx := context.Background()

	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

  if opt.Path_xroot == "" {
		opt.Path_xroot = "root"
	}

  if opt.Port == "" {
		opt.Port = "1094"
	}

	if opt.User == "" {
		opt.User = "localhost"
	}

  if opt.Path_to_file == "" {
		opt.Path_to_file = "/"
	}

  //path_name =  opt.Path_xroot + "://" + opt.User + ":" + opt.Port + "/" + opt.Path_to_file,
  url := opt.Path_xroot + "://" + opt.User + ":" + opt.Port + "/" + opt.Path_to_file +"/" + root

    f := &Fs{
    name:      name,
    root:      root,
    opt:       *opt,
   // m:         m,
  //    url:
    url:       url,
  //    pacer:       fs.NewPacer(pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
  }


	f.features = (&fs.Features{
    CanHaveEmptyDirectories: true,
  }).Fill(f)

  cli,path,err := f.xrdremote(url, ctx)
  //fmt.Printf("func Newfs: path= %q & err= %w\n",path, err) //commentaire
  /*  fi,path,fsx,err := f.connectxrootclient(f.url, ctx)*/
  if err != nil {
    return nil, errors.Wrap(err, "NewFs")
  }
  defer cli.Close()


  if root != "" {
		// Check to see if the root actually an existing file
		remote := filepath.Base(path)
		f.root = filepath.Dir(path)
    fmt.Println("newfs baseroot=",remote) //commentaire
		if f.root == "." {
			f.root = ""
		}
		_, err := f.NewObject(ctx, remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound || errors.Cause(err) == fs.ErrorNotAFile {
				// File doesn't exist so return old f

				f.root = path
        fmt.Println("retourne old Fs= ", f, " root = ",path)
				return f, nil
			}
			return nil, err
		}
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}

  /*if root != "" {
    f.root = url

  }*/
  //xrdfile, err := xrdio.Open("root://localhost//tmp/back2/data.txt")
  //fmt.Printf("xrdfile  type = %T \n ", xrdfile)  //commentaire

//fmt.Printf("  Newfs: 238. f.root= %q \n",f.root)  //commentaire
  //return NewFsWithConnection(ctx, name, root, m, opt)
  return f, nil
}



// ok
// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}


//ok
func (f *Fs) Features() *fs.Features {
	return f.features
}


// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA1)
  //return hash.Supported()
}




/*
// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.Item) (fs.Object, error) {
  o := &Object{
		fs:     f,
		remote: remote,
	}
	var err error
	if info != nil {
		// Set info
		err = o.setMetaData(info)
	} else {
		err = o.readMetaData(ctx) // reads info and meta, returning an error
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}*/








// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs NewObject \n")
  }
	o := &Object{
		fs:     f,
		remote: remote,
	}
  fmt.Println("Newobject fs = ",f," remote=",remote)  //commentaire
	err := o.stat()
	if err != nil {
		return nil, err
	}
  fmt.Println("Newobject2 = ",o)  //commentaire
	return o, nil
}



// setMetadata sets the file info from the os.FileInfo passed in
func (o *Object) setMetadata(info os.FileInfo) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction object setMetadata \n")
  }
  if o.size != info.Size() {
		o.size = info.Size()
    //
	}
	if !o.modTime.Equal(info.ModTime()) {
		o.modTime = info.ModTime()
	}
	if o.mode != info.Mode() {
		o.mode = info.Mode()
	}
/*  fmt.Printf("modtime= %q \n",o.modTime) //commentaire
  fmt.Printf("o.size = %d \n",o.size )
  fmt.Printf("o.mode= %q \n",o.mode)*/
}


func (f *Fs) display(ctx context.Context, fsx xrdfs.FileSystem, root string, info os.FileInfo, dir string ) (entries fs.DirEntries, err error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction display \n")
  }

  dirt := path.Join(root, info.Name())
/*  if !info.IsDir() {  // if a revoir
    //name := strings.SplitAfter(dirt, "/")
    //remote := name[len(name)-1]
    o := &Object{
      fs:     f,
      remote: ".",
    }

    o.setMetadata(info)
    entries = append(entries, o)
    return entries,nil
	}*/


	ents, err := fsx.Dirlist(ctx, dirt)

	if err != nil {
		return nil,fmt.Errorf("could not list dir %q: %w", dirt, err)
	}

	//o := tabwriter.NewWriter(os.Stdout, 8, 4, 0, ' ', tabwriter.AlignRight)

	for _, info := range ents {
    remote := path.Join(dir, info.Name())
    //remote := e.Name()
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
    //fmt.Println("entries = ", entries) //commentaire
    //format(o, dir, e, long)
	}


  //fmt.Printf("entries  type = %T \n ", entries)  //commentaire
	return entries,nil
}

/*
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs list \n")
  }
  fmt.Printf("utilisation de list avec le chemin %q & url=%q \n", dir,f.root) //commentaire
  return nil,nil
}
*/
// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs list \n")
  }

  //fmt.Printf("utilisation de list avec le chemin %q & url=%q \n", dir,f.url) //commentaire

  xrddir := path.Join(f.root, dir)
  //xrddir :=  f.url + dir  //test
  //xrddir := "root://localhost/tmp/back2"
  //fmt.Printf("List xrddir= %q \n",xrddir) //a retirer //commentaire
  //fi,urlpath,fsx,err := f.connectxrootclient(dir, ctx)
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
		return nil, fs.ErrorDirNotFound  //errors.Wrap(err," could not stat" + url.Path )
	}
  entries,err = f.display(ctx, fsx, path, fi, dir )
  //fmt.Printf("entries  type = %T \n ", entries)  //a retirer

  return entries,err
}





func (f *Fs) Mkdir(ctx context.Context, dir string) error {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs mkdir \n")
  }
  xrddir := path.Join(f.root, dir)
  client,path,err :=f.xrdremote(xrddir,ctx)
  if err != nil{
    return err
  }
  defer client.Close()

  err = os.MkdirAll(path, 0755)
  //err = os.Mkdir(path, 0755

  if err != nil {
    return err
  }
  return nil
}


// Rmdir deletes the root folder
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs rmdir \n")
  }
	// Check to see if directory is empty

  //fmt.Printf("utilisation rmdir path= %q \n", dir)  //commentaire

	entries, err := f.List(ctx, dir)
	if err != nil {
		return errors.Wrap(err, "Rmdir")
	}
	if len(entries) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	// Remove the directory
  xrddir := path.Join(f.root, dir)
  client,path,err :=f.xrdremote(xrddir,ctx)
  if err != nil{
    return err
  }
  defer client.Close()

  err = client.FS().RemoveDir(ctx, path)
  return err
}


func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs Move \n")
  }

  srcObj, ok := src.(*Object)
  if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

  xrddir := path.Join(f.root, remote)
  fmt.Println("xrddir = ",xrddir)  //commentaire
  client,path,err :=f.xrdremote(xrddir,ctx)
  if err != nil{
    return nil, errors.Wrap(err, "Move")
  }
  defer client.Close()

  fmt.Println("srcObj.path() = ",srcObj.path()," path = ",path)  //commentaire
  //err = client.FS().Rename(ctx, srcObj.path(), xrddir);
  err = client.FS().Rename(ctx, srcObj.path(), path);
  if err != nil {
		return nil, errors.Wrap(err, "Move Rename failed")
	}

  dstObj, err := f.NewObject(ctx, remote)
  if err != nil {
		return nil, errors.Wrap(err, "Move NewObject failed")
	}
  return dstObj, nil
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
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs DirMove \n")
  }
  srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
  srcPath := path.Join(srcFs.root, srcRemote)
	dstPath := path.Join(f.root, dstRemote)


  client,path,err :=f.xrdremote(dstPath,ctx)
  if err != nil{
    return errors.Wrap(err, "dirMove")
  }
  defer client.Close()

  // Make sure the parent directory exists


	err = os.MkdirAll(path, 0755)
	if err != nil {
		return errors.Wrap(err, "DirMove mkParentDir dst failed")
	}


  err = client.FS().Rename(ctx, srcPath , dstPath);
  if err != nil {
  		return errors.Wrapf(err, "DirMove Rename(%q,%q) failed", srcPath, dstPath)
  }

  return  nil
}


func (f *Fs) Precision() time.Duration {
	return time.Second
}


// Put data from <in> into a new remote sftp file object described by <src.Remote()> and <src.ModTime(ctx)>
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs put \n")
  }
  fmt.Println("put src.Remote() ok = ",src.Remote(), "in = ", in)
  // Temporary Object under construction - info filled in by Update()

	//o,err := f.NewObject(ctx, src.Remote())
  o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
  //fmt.Println("put o ok = ",o)  //commentaire
	err := o.Update(ctx, in, src, options...)
	if err != nil {
		return nil, err
	}
	return o, nil
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
func (f *Fs) stat(remote string) (info os.FileInfo, err error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction fs stat \n")
  }

  ctx := context.Background()
  xrddir := path.Join(f.root, remote)
  //xrddir := path.Join("/tmp/back2/ancien", remote)  // a retirer
  fmt.Println("stat fs f.root = ",f.root,"  xrddir=", xrddir)
  //xrddir :=  f.url + remote  //test
//  fmt.Printf("(f *fs) Stat xrddir= %q \n",xrddir)   //a retirer  //commentaire
  client,path,err :=f.xrdremote(xrddir,ctx)
  if err != nil{
    return nil, fmt.Errorf("could not stat %q: %w", path, err)
  }
  defer client.Close()
  fsx := client.FS()
  info,err = fsx.Stat(ctx,path)
  fmt.Println("stat fs info = ",info, " path= ",path)  //commentaire
	return info, err
}


// stat updates the info in the Object
func (o *Object) stat() error {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction object stat \n")
  }
	info, err := o.fs.stat(o.remote)

	if err != nil {

		//if os.IsNotExist(err) {
  //    fmt.Printf("appel fct stat 2 \n")
	//		return fs.ErrorObjectNotFound
//		}
		//return errors.Wrap(err, "stat failed")
    return fs.ErrorObjectNotFound
	}
	if info.IsDir() {
		return errors.Wrapf(fs.ErrorNotAFile, "%q", o.remote)
	}
	o.setMetadata(info)
	return nil
}




// readMetaData gets the metadata if it hasn't already been fetched

//

// it also sets the info
/*
func (o *Object) readMetaData(ctx context.Context) (err error) {

	if o.hasMetaData {
		return nil
	}
  info, err := o.getMetaData(ctx)
	if err != nil {
		return err
	}
	return o.setMetaData(info)
}
*/


// setMetaData sets the metadata from info
/*func (o *Object) setMetaData(info *api.Item) (err error) {

	if info.Type != api.ItemTypeFile {
		return errors.Wrapf(fs.ErrorNotAFile, "%q is %q", o.remote, info.Type)
	}
  o.hasMetaData = true
  o.modTime = fi.ModTime()
  o.sha1 = info.SHA1
	o.size = fi.Size()
	o.mode = fi.Mode()
	return nil
}
*/

func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}


// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}



// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
  //fmt.Printf("o.Size  \n")  //commentaire
	return o.size
}



// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}


// Fs is the filesystem this remote sftp file object is located within
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns the SHA-1 of an object returning a lowercase hex string

func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction object hash \n")
  }
	if t != hash.SHA1 {
		return "", hash.ErrUnsupported
	}
	return o.sha1, nil
}


// path returns the native path of the object
func (o *Object) path() string {
	return path.Join(o.fs.root, o.remote)
}


// objectReader represents a file open for reading on the xrootd server
type objectReader struct {
	o    *Object           // object that is open
//	in   io.ReadCloser     // handle we are wrapping
//	hash *hash.MultiHasher // currently accumulating hashes
	xrdFile   *xrdio.File        // file object reference
}


func (file *objectReader) Read(p []byte) (n int, err error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction objectreader Read \n")
  }
  n,err = file.xrdFile.Read(p)
  return n,nil
}

func (file *objectReader) Close() (err error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction objectreader Close \n ")
  }
  err = file.xrdFile.Close()
  return err
}
/*
func newObjectReader(xrdfile *xrootd.file) *objectReader {


}
*/
/*
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
  return nil, nil
}
*/
/*
func newFadviseReadCloser(o *Object, f *os.File, offset, limit int64) io.ReadCloser {
	return f
}
*/
// Open an object for read



func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction object open \n ")
  }
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
  //xrddir := o.path()
//  client,path,err := o.fs.xrdremote(o.path(),ctx)
//  if err != nil{
//    return nil, fmt.Errorf("could not stat %q: %w", path, err)
//  }
//  defer client.Close()
//  fsx := client.FS()
  //xrdfile, err := client.FS().Open(ctx, path,xrdfs.OpenModeOwnerRead, xrdfs.OpenOptionsOpenRead)
//  xrdfile, err := fsx.Open(ctx, path,xrdfs.OpenModeOwnerRead, xrdfs.OpenOptionsOpenRead)  //xrdfile type = *xrootd.file
//  if err != nil {
//    return nil, errors.Wrap(err, "Open failed")
//  }
  xrdfile, err := xrdio.Open(o.path())
    fmt.Println("xrdfile = ",xrdfile)  //commentaire

  //fsx := client.FS()
  //fi,err := fsx.Stat(ctx,path)

  if offset > 0 {
		off, err := xrdfile.Seek(offset, io.SeekStart)
		if err != nil || off != offset {
			return nil, errors.Wrap(err, "Open Seek failed")
		}
	}

	// Update the hashes as we go along
//  in = &xrdOpenFile{
//		o:    o,
//		in:   wrappedFd,
//		xrdfile:   xrdfile,
//	}

  /*
  objectReader := &objectReader{
    o: o,
    xrdFile: xrdfile,
  }
  in = readers.NewLimitedReadCloser(objectReader, limit)
  */
  in = readers.NewLimitedReadCloser(xrdfile, limit)
  fmt.Println("in = ",in)  //commentaire
  //fmt.Printf("in  contenue = %q \n ", in)  //a retirer
  //fmt.Printf("in  type = %t \n ", in)  //a retirer
	return in, nil
}






// SetModTime sets the modification and access time to the specified time
//
// it also updates the info field
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction object sedmodtime \n")
  }
/*  if !o.fs.opt.SetModTime {
 		return nil
 	}
*/
/*client,path,err :=o.fs.xrdremote(o.path(),ctx)
if err != nil{
  return err
}
defer client.Close()*/
  err := os.Chtimes(o.path(), modTime, modTime)
  if err != nil {
		return err
	}
  err = o.stat()
	if err != nil {
		return errors.Wrap(err, "SetModTime stat failed")
	}
	return nil
}



// Storable returns a boolean showing if this object is storable
func (o *Object) Storable() bool {
	return false
}

// Update the object from in with modTime and size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction object update \n")
  }


  client,path,err := o.fs.xrdremote(o.path(),ctx)
  if err != nil{
    return errors.Wrap(err, "Update: open client failed")
  }
  defer client.Close()

  //err = os.MkdirAll(path, 0777)
  if err != nil {
    return errors.Wrap(err, "could not create output directory")
	}

  fsx := client.FS()

  file, err := xrdio.OpenFrom(fsx, path)
	if err != nil {
		return err
	}
	defer file.Close()

//  file, err := client.FS().Open(ctx, path,xrdfs.OpenModeOwnerRead, xrdfs.OpenOptionsOpenRead)
//  if err != nil {
//    return errors.Wrap(err, "update: Open file failed")
//  }


//  file, err := xrdio.Open(o.path())
  fmt.Printf("ouverture? \n")
  //_, err = io.Copy(file, in)



  _, err = io.CopyBuffer(file, in, make([]byte, 16*1024*1024))

	if err != nil {
	 return errors.Wrap(err, "update: could not copy to output file")
  fmt.Printf("copy ok? \n")  //commentaire
	}
//  _, err =  file.WriteAt([]byte(in), 0)


  err = file.Close()

  if err != nil{
    return errors.Wrap(err, "update: close file failed")
  }


  err = o.SetModTime(ctx, src.ModTime(ctx))
	if err != nil {
		return errors.Wrap(err, "Update SetModTime failed")
	}
	return nil
}


// Remove a remote sftp file object

func (o *Object) Remove(ctx context.Context) error {
  if titre_fonction == true{
    fmt.Printf("Utilisation de la fonction object remove ")
  }
  //xrddir := path.Join(f.root, dir)
  client,path,err :=o.fs.xrdremote(o.path(),ctx)
  if err != nil{
    return err
  }
  defer client.Close()

  err = client.FS().RemoveFile(ctx, path);

  if  err != nil {
      return err
  }

  return nil
}



var (
    _ fs.Fs          = &Fs{}
//  	_ fs.Mover       = &Fs{}
//  	_ fs.DirMover    = &Fs{}
//  	_ fs.Object      = &Object{}
)

//URL
// Addr string // address (host [:port]) of the server
// User string // user name to use to log in
// Path string // path to the remote file or directory

