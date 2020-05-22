---
title: "XrootD"
description: "XrootD"
date: "2020-05-22"
---

<i class="fa fa-server"></i> XrootD
----------------------------------------

[XRootD](https://xrootd.slac.stanford.edu/) is a storage service

Paths are specified as `remote:path`. If the path does not begin with
a `/` it is relative to the home directory of the user.  An empty path
`remote:` refers to the directory defined during initialization.

Here is an example of making a XrootD configuration.  First run

    rclone config
    
This will guide you through an interactive setup process.

```
No remotes found - make a new one
n) New remote
s) Set configuration password
q) Quit config
n/s/q> n
name> remote
Type of storage to configure.
Choose a number from below, or type in your own value
[snip]
XX / xrootd-client
   \ "xrootd"
[snip]
Storage> xrootd
** See help for xrootd backend at: https://rclone.org/xrootd/ **

xrootd host to connect to (probably 'root' )
Enter a string value. Press Enter for the default ("").
path_xroot> root
xrootd username (default 'localhost')
Enter a string value. Press Enter for the default ("").
user> Xrootd port, leave blank to use default (1094)
Enter a string value. Press Enter for the default ("").
port>Xrootd root path, example (/tmp) and default '/'
Enter a string value. Press Enter for the default ("").
path_to_file> /tmp
Remote config
--------------------
[remote]
type = xrootd
path_xroot = root
path_to_file = /tmp
--------------------
y) Yes this is OK (default)
e) Edit this remote
d) Delete this remote
y/e/d> y
```

This remote is called `remote` and can now be used like this:

See all directories in the home directory

    rclone lsd remote:

Make a new directory

    rclone mkdir remote:path/to/directory
    
List the contents of a directory

    rclone ls remote:path/to/directory
