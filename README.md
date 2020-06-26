# Large Version Control System
This is a version control system for large binary files, as opposed to Git/SVN,
which are meant for text files.

It works by hashing every file from the target directory and comparing each hash to the
files that already exist in its repository. It then only copies the files that have changed,
so you never have duplicates of files under any circumstance.

Each "patch" (think: commit) is identified by a tag and has a list of the files necessary
along with their original path.

## Usage Examples
These will be added soon (probably)

## Can I use it in my project?
Absolutely! Except, it isn't extremely well-tested, so use at your own risk.