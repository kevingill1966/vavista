; M routines, based on work in the GT.M manual file gtmcip_gtmaccess.zip, and the pyGTMx python module.
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



