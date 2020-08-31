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

xrootd servername, leave blank to use default
Enter a string value. Press Enter for the default ("localhost").
servername> 
Xrootd port, leave blank to use default
Enter a string value. Press Enter for the default ("1094").
port>
Xrootd root path, example (/tmp)
Enter a string value. Press Enter for the default ("/").
path_to_file> /tmp
username
Enter a string value. Press Enter for the default ("userxroot").
user>
Choice of type of checksum:
Enter a string value. Press Enter for the default ("adler32").
Choose a number from below, or type in your own value
 1 /
   \ "adler32"
 2 / no Checksum
   \ "none"
hash_chosen> 2
Edit advanced config? (y/n)
y) Yes
n) No (default)
y/n> n
Remote config
--------------------
[remote]
type = xrootd
servername = localhost
path_to_file = /tmp
hash_chosen = none
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
