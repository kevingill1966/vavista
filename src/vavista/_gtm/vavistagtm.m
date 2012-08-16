; M routines, based on work in the GT.M manual file gtmcip_gtmaccess.zip, and the pyGTMx python module. ; 8/16/12 5:20pm
;
%vavistagtm  ; entry points to access GT.M
    write "vavistagtm entry point - see documentation for usage of this code"
    quit
    ;
mexec(cmd,s0,s1,s2,s3,s4,s5,s6,s7,l0,l1,l2,l3,l4,l5,l6,l7,d0,d1,d2,d3,d4,d5,d6,d7,sRv,iRv,dRv)
    ; I have an idea that I can set up one generic function and execute arbitrary routines using it.
    ; If this works, I don't need the other functions
    ; Limit 32 paramters
    ; cmd - 1
    ; return values 3
    ; 3 x 8 in/out vars
    xecute cmd
    quit:$quit 0 quit
    ;
;; The performance of the above code was unimpressive. I am attempting
;; to compile common usages here
mget(s0,s1)
    set s1=@s0
    quit:$quit 0 quit
    ;
mset(s0,s1)
    set @s0=s1
    quit:$quit 0 quit
    ;

mord(s0,s1)
    ; Order through a global - note that the key returned
    ; is the last part of the key
    s s1=$order(@s0)
    quit:$quit 0 quit
    ;
ddwalk(s0,s1,s2,s3,s4)
    ; walk the data dictionary nodes
    ; s0 - fieldid  (in/out)
    ; s1 - field info  
    ; s2 - file number(in)
    ; s3 - title
    ; s4 - help
    set s0=$order(^DD(s2,s0)) Q:s0'=+s0  s s1=$G(^DD(s2,s0,0)),s3=$G(^DD(s2,s0,.1)),s4=$G(^DD(s2,s0,3))
    quit:$quit 0 quit
    ;
;
;GTM>zwrite @s0@(6820)
;^DIC(19,"B","zzmas",6820)=""
;
;GTM>set s0="^DIC(19,""B"")"          
;
;GTM>zwrite @s0@("zzmas",6820)
;^DIC(19,"B","zzmas",6820)=""
;
; set s=ord(0,"^DIC(19,""B"",""zzmas"")",0)
;set s0=$order(row173008108("19","6963,",s0)),l0=0 if s0'="" set l0=$data(row173008108("19","6963,",s0)),s1=$GET(row173008108("19","6963,",s0))
; root = "row173008108(""19"",""6963,"")"
; key=0
; 
; order through a global - return key, data, value for the next item
glwalk(ref,key,data,value) ; Order through a global
    s key=$o(@ref@(key)),data=0 if key'="" set data=$data(@ref@(key)),value=$GET(@ref@(key))
    quit:$quit 0 quit
    ;



;; This was an attempt at transaction management. It did not work because
;; you cannot exit a function within a transaction. Furthermore, you cannot
;; callback into GT.M during a transaction. Transactions are therefore
;; disabled in GT.M. I am leaving the concept here in the hope that they
;; will work with Cache. I am leaving calls here so that I can see the
;; transaction bracketing in the GT.M Journal files.
tstart
    ; transaction management
    ; TSTART ():SERIAL
    ZTSTART
    quit:$quit 0 quit
    ;
tcommit
    ; transaction management
    ; TCOMMIT
    ZTCOMMIT
    quit:$quit 0 quit
    ;
trollback
    ; transaction management
    ; TROLLBACK
    ZTCOMMIT
    quit:$quit 0 quit
    ;

;; The rest of this file is for testing the interface. It does not comprise part of the 
;; implementation.
testproc(s0,l0,d0)
    set s0="testproc"
    set l0=1111
    set d0=222.22
    quit
    ;
testfunc(s0,l0,d0)
    set s0="testfunc"
    set l0=3333
    set d0=444.44
    quit 99
    ;
testref(s0,s1)
    set s0=s1
    quit:$quit 0 quit
    ;



