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



  "go-hep.org/x/hep/xrootd"
  "go-hep.org/x/hep/xrootd/xrdio"
  "go-hep.org/x/hep/xrootd/xrdfs"


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
  titre_fonction = false
  maxSizeForCopy = 5 * 1024 * 1024  // The maximum size of object we can COPY
)


// Globals
var (
)

// Register with Fs
func init(){
  fsi :=&fs.RegInfo{
    Name:        "xrootd",
    Description: "xrootd-client",
    NewFs:       NewFs,

    Options: []fs.Option{{
    Name:     "path_xroot",
    Help:     "xrootd host to connect to (probably 'root' )",
    Required: true,

    }, {
  			Name: "user",
  			Help: "xrootd username (default 'localhost') ",
      }, {
    		Name: "port",
    		Help: "Xrootd port, leave blank to use default (1094)",
      }, {
        Name: "path_to_file",
        Help: "Xrootd root path, example (/tmp) and default '/'",
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
  //m            configmap.Mapper // config
  url          string
  features     *fs.Features          // optional features
  objectHashesMu sync.Mutex // global lock for Object.hashes
}


type Object struct {
	fs            *Fs           // what this object is part of
  remote        string       // The remote path
	size          int64       // size of the object
	modTime       time.Time   // modification time of the object if known
  mode          os.FileMode
//  sha1          string    // SHA-1 of the object content
  hashes         map[hash.Type]string // Hashes
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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction newfs  ")
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

  url := opt.Path_xroot + "://" + opt.User + ":" + opt.Port + "/" + opt.Path_to_file +"/" + root

    f := &Fs{
    name:      name,
    root:      root,
    opt:       *opt,
    //m:         m,
    url:       url,
  //pacer:       fs.NewPacer(pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
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

/*func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA1)
}*/


func (f *Fs) Hashes() hash.Set {
	return hash.Supported()
}

// NewObject finds the Object at remote.  If it can't be found
//
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction fs NewObject  ")
  }
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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction object setMetadata  ")
  }
  if o.size != info.Size() {
		o.size = info.Size()
	}
	if !o.modTime.Equal(info.ModTime()) {
		o.modTime = info.ModTime()
	}
	if o.mode != info.Mode() {
		o.mode = info.Mode()
	}
}


//Continuation of the List function
func (f *Fs) display(ctx context.Context, fsx xrdfs.FileSystem, root string, info os.FileInfo, dir string ) (entries fs.DirEntries, err error) {
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction display  ")
  }

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
    fmt.Println("Utilisation de la fonction fs list  ")
  }

  //fmt.Printf("utilisation de list avec le chemin %q & url=%q \n", dir,f.url) //commentaire

  xrddir := path.Join(f.root, dir)
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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction fs mkdir  ")
  }
  xrddir := path.Join(f.root, dir)
  client,path,err :=f.xrdremote(xrddir,ctx)
  if err != nil{
    return err
  }
  defer client.Close()

  err = os.MkdirAll(path, 0755)

  if err != nil {
    return err
  }

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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction fs rmdir  ")
  }

	// Check to see if directory is empty
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
  if  err != nil {
      return err
  }

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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction fs Purge  ")
  }

  client,path,err := f.xrdremote(f.root,ctx)
  if err != nil{
    return err
  }
  defer client.Close()

  err = client.FS().RemoveAll(ctx, path);
  if  err != nil {
      return err
  }

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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction fs Move  ")
  }

  srcObj, ok := src.(*Object)
  if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

  xrddir := path.Join(f.root, remote)

  client,path,err :=f.xrdremote(xrddir,ctx)
  if err != nil{
    return nil, errors.Wrap(err, "Move")
  }
  defer client.Close()

  err = client.FS().Rename(ctx, srcObj.path(), path);
  if err != nil {
		return nil, errors.Wrap(err, "Move Rename failed")
	}

  dstObj, err := f.NewObject(ctx, remote)
  if err != nil {
		return nil, errors.Wrap(err, "Move NewObject failed")
	}

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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction fs DirMove  ")
  }

  srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

  srcPath := path.Join(srcFs.root, srcRemote)
  dstPath := path.Join(f.root, dstRemote)

  // Check if destination exists
  ok, err := f.dirExists(ctx,dstPath)
  if ok {
    return fs.ErrorDirExists
  }


  client,path,err :=f.xrdremote(dstPath,ctx)
  if err != nil{
    return errors.Wrap(err, "dirMove not open client")
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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction fs put")
  }

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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction fs stat  ")
  }

  xrddir := path.Join(f.root, remote)

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

  err = client.Close();
  if  err != nil {
      return info,err
  }

	return info, nil
}


// stat updates the info in the Object
func (o *Object) stat(ctx context.Context) error {
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction object stat  ")
  }
	info, err := o.fs.stat(ctx, o.remote)

	if err != nil {

		//if os.IsNotExist(err) {
    //   fmt.Printf("appel fct stat 2 \n")
	  //	return fs.ErrorObjectNotFound
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


// Hash returns the requested hash of a file as a lowercase hex string
func (o *Object) Hash(ctx context.Context, r hash.Type) (string, error) {
	// Check that the underlying file hasn't changed
	oldtime := o.modTime

	oldsize := o.size

	err := o.stat(ctx)
	if err != nil {
		return "", errors.Wrap(err, "hash: failed to stat")
	}

	o.fs.objectHashesMu.Lock()
	hashes := o.hashes
	hashValue, hashFound := o.hashes[r]
	o.fs.objectHashesMu.Unlock()

	if !o.modTime.Equal(oldtime) || oldsize != o.size || hashes == nil || !hashFound {
		var in io.ReadCloser

      in, err = xrdio.Open(o.path())
      if err!= nil{
        return "", errors.Wrap(err, "Hash open failed")
      }

  		hashes, err = hash.StreamTypes(in, hash.NewHashSet(r))
  		closeErr := in.Close()
  		if err != nil {
  			return "", errors.Wrap(err, "hash: failed to read")
  		}
  		if closeErr != nil {
  			return "", errors.Wrap(closeErr, "hash: failed to close")
  		}

  		hashValue = hashes[r]
  		o.fs.objectHashesMu.Lock()
  		if o.hashes == nil {
  			o.hashes = hashes
  		} else {
  			o.hashes[r] = hashValue
  		}
  		o.fs.objectHashesMu.Unlock()
	}
	return hashValue, nil
}


// path returns the native path of the object
func (o *Object) path() string {
	return path.Join(o.fs.root, o.remote)
}





// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction object open   ")
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

  xrdfile, err := xrdio.Open(o.path())
  if err!= nil{
    return nil, errors.Wrap(err, "Open failed")
  }


  if offset > 0 {
		off, err := xrdfile.Seek(offset, io.SeekStart)
		if err != nil || off != offset {
			return nil, errors.Wrap(err, "Open Seek failed")
		}
	}

  in = readers.NewLimitedReadCloser(xrdfile, limit)
	return in, nil
}




// SetModTime sets the modification and access time to the specified time
//
// it also updates the info field
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction object setModtime  ")
  }

  client,path,err :=o.fs.xrdremote(o.path(),ctx)
  if err != nil{
    return errors.Wrap(err, "SetModTime")
  }
  defer client.Close()

  err = os.Chtimes(path, modTime, modTime)
  if err != nil {
		return errors.Wrap(err, "SetModTime failed")
	}
  err = o.stat(ctx)
	if err != nil {
		return errors.Wrap(err, "SetModTime stat failed")
	}
	return nil
}


// Storable returns a boolean showing if this object is storable
func (o *Object) Storable() bool {
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction object Storable  ")
  }

	return o.mode.IsRegular()
}


// Update the object from in with modTime and size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction object update ctx= ",ctx)
  }

  o.hashes = nil

  err = os.MkdirAll(filepath.Dir(o.path()), 0755)


  out, err := os.Create(o.path())
  if err != nil {
		return err
	}

  // remove the file if upload failed

	remove := func() {
    client,path,removeErr :=o.fs.xrdremote(o.path(),ctx)
    if removeErr != nil{
      	fs.Debugf(src, "Failed to open client", removeErr)
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
    }
	}

  _, err = io.CopyBuffer(out, in, make([]byte, maxSizeForCopy))

	if err != nil {
    remove()
    return errors.Wrap(err, "update: could not copy to output file")
	}

  err = out.Close()
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
  if titre_fonction == true{
    fmt.Println("Utilisation de la fonction object remove ")
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

  err = client.Close();
  if  err != nil {
      return err
  }
  return nil
}



// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.PutStreamer    = &Fs{}
  	_ fs.Mover       = &Fs{}
  	_ fs.DirMover    = &Fs{}
  	_ fs.Object      = &Object{}
)
