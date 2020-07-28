// Package xrootd provides a filesystem interface using github.com/go-hep/hep/tree/master/xrootd


package xrootd

import(
  "context"
  "time"
	"io"
  "os"
  "path"
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
)

// Constants
const (
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
    //config: func(name string, m )

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
	fs      *Fs           // what this object is part of
	remote  string       // The remote path
	size    int64       // size of the object
	modTime time.Time   // modification time of the object if known
  mode         os.FileMode

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
  ctx := context.Background()
  fmt.Printf("utilisation de Newfs\n")
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
  url := opt.Path_xroot + "://" + opt.User + ":" + opt.Port + "/" + opt.Path_to_file

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
  fmt.Printf("func Newfs: path= %q & err= %w\n",path, err)
  /*  fi,path,fsx,err := f.connectxrootclient(f.url, ctx)*/
  if err != nil {
    return nil, errors.Wrap(err, "NewFs")
  }
  defer cli.Close()
//  url, err := xrdio.Parse(paht_name)
//  if err != nil {
//    return fmt.Errorf("could not parse %q: %w", name, err)
//  }
//  c, err := xrootd.NewClient(ctx, url.Addr, url.User)  // c = client
//  if err != nil {
//  	return fmt.Errorf("could not create client: %w", err)
//  }
  fmt.Printf("226. f.root= %q \n",f.root)
  if f.root ==""{
    f.root = path
  }
  /*if root != "" {
    f.root = url

  }*/
  fmt.Printf("234. f.root= %q \n",f.root)
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

// A revoir?
func (f *Fs) Hashes() hash.Set {
	return hash.Supported()
}




// NewObject creates a new remote sftp file object

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return nil, nil
}







// setMetadata sets the file info from the os.FileInfo passed in
func (o *Object) setMetadata(fi os.FileInfo) {
	o.modTime = fi.ModTime()
	o.size = fi.Size()
	o.mode = fi.Mode()
}


func (f *Fs) display(ctx context.Context, fsx xrdfs.FileSystem, root string, fi os.FileInfo /*, long, recursive bool*/) (entries fs.DirEntries, err error) {
	/*end := ""
	if recursive {
		end = ":"
	}*/
  fmt.Printf("Utilisation display")
	dir := path.Join(root, fi.Name())
	//fmt.Printf("%s%s\n", dir, end)
/*
	if long {
		fmt.Printf("total %d\n", fi.Size())
	}
*/

	ents, err := fsx.Dirlist(ctx, dir)

	if err != nil {
		return nil,fmt.Errorf("could not list dir %q: %w", dir, err)
	}

	//o := tabwriter.NewWriter(os.Stdout, 8, 4, 0, ' ', tabwriter.AlignRight)

	for _, e := range ents {
    remote := path.Join(root, e.Name())

    if e.IsDir() {
			d := fs.NewDir(remote, e.ModTime())
			entries = append(entries, d)
		} else {
			o := &Object{
				fs:     f,
				remote: remote,
			}
			o.setMetadata(fi)
			entries = append(entries, o)
		}

    //format(o, dir, e, long)
	}
//	o.Flush()
/*	if recursive {
		for _, e := range ents {
			if !e.IsDir() {
				continue
			}
			// make an empty line before going into a subdirectory.
			fmt.Printf("\n")
		//	entries, err := display(ctx, fsx, dir, e, long, recursive)
			if err != nil {
				return nil, err
			}
		}
	}*/

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

  fmt.Printf("utilisation de list avec le chemin %q & url=%q \n", dir,f.url)
  if dir == "" {
    //dir = "."
		dir = "/back2"  //test
	}
//  xrddir := path.Join( f.url, dir)
  xrddir :=  f.url + dir  //test

  fmt.Printf("xrddir= %q \n",xrddir)
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
  return f.display(ctx, fsx, path, fi /*, false, false*/)
}





func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return nil
}


// Rmdir deletes the root folder
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return nil
}

func (f *Fs) Precision() time.Duration {
	return time.Second
}


// Put data from <in> into a new remote sftp file object described by <src.Remote()> and <src.ModTime(ctx)>
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil,nil
}


// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}


// String converts this Fs to a string (String returns the URL for the filesystem)
func (f *Fs) String() string {
	return ""
}



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



//URL
// Addr string // address (host [:port]) of the server
// User string // user name to use to log in
// Path string // path to the remote file or directory

