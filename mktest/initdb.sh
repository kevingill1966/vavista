
# To test the fileman system, you need to create a mumps system.

# After running gtmprofile, you should have the following setup...

. ./setup_env

rm -rf $root
mkdir $root
mkdir $root/o
mkdir $root/r
mkdir $root/g

# Copy the mumps source code from the git tree. 
ln -s $gitroot/Scripts/*m $root/r
find "$gitroot/Packages/Kernel/Routines/" -name \*m -print | xargs -I '{}' ln -s '{}' $root/r
find "$gitroot/Packages/Toolkit/Routines/" -name \*m -print | xargs -I '{}' ln -s '{}' $root/r
find "$gitroot/Packages/VA FileMan/Routines/" -name \*m -print | xargs -I '{}' ln -s '{}' $root/r

mumps -r ^GDE  <<DONE
change -segment DEFAULT -file=$root/g/mumps.dat
change -segment DEFAULT -block_size=4096 -global_buffer_count=2048
change -segment DEFAULT -allocation=150000 -extension=20000
change -region DEFAULT -key_size=255 -record_size=4080
exit
DONE

mupip create

rm -f $root/g/*.mj[oe]
$gtm_dist/mupip set -journal="enable,on,before" -file $root/g/mumps.dat
find $root -name "*mjl_*" -mtime +3 -exec rm -v {} \;


# Copy the mumps globals from the git tree
ls "$gitroot/Packages/Kernel/Globals" > globals.lst
echo 'do LIST^ZGI("globals.lst","'$HOME'/VistA-FOIA/Packages/Kernel/Globals/")' | mumps -direct
ls "$gitroot/Packages/Toolkit/Globals" > globals.lst
echo 'do LIST^ZGI("globals.lst","'$HOME'/VistA-FOIA/Packages/Toolkit/Globals/")' | mumps -direct
ls "$gitroot/Packages/VA FileMan/Globals" > globals.lst
echo 'do LIST^ZGI("globals.lst","'$HOME'/VistA-FOIA/Packages/VA FileMan/Globals/")' | mumps -direct
rm globals.lst

# this prints out lots of errors
mumps -r ^ZTMGRSET

# this is not necessary as the skeleton data is loaded from the OSHERA repository
#mumps -r ^DINIT << DONE
#YES
#TEST DB
#88
#GT.M(UNIX)
#DONE
