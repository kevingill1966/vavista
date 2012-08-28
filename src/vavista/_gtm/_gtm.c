/*
    This is the gtm compatibility layer. I expect to implement a Cache alternative
    at some stage.

    This code was heavily influenced by pyGTMx.
*/
#include <Python.h>
#include "structmember.h"

#include <string.h>
#include <time.h>
#include <termios.h>
#include <unistd.h>

#include "gtmxc_types.h"

#define MAXMSG 2048      /* max length of a GT.M message */
#define MAXVAL 1048576  /* max length of a value GT.M can return */

static gtm_char_t     msgbuf[MAXMSG];
static gtm_status_t   status;
static int gInitialised=0;

FILE *fpLog = NULL;

/* GT.M call wrapper - see example code in GT.M manual
 */
#define CALLGTM(xyz) \
    status = (xyz) ;     \
    reset_input_mode(); \
    if (0 != status ) {               \
        gtm_zstatus(msgbuf, MAXMSG); \
        PyErr_SetString(GTMException, msgbuf); \
        return NULL; \
    }

/* From pyGTMx --------------------------------------------------------------------*/
struct termios term_settings;  

void reset_input_mode(void) {
    if (isatty(STDIN_FILENO)) {
        tcsetattr(STDIN_FILENO, TCSANOW, &term_settings);
    } 
}

void save_input_mode(void) {
    if (isatty(STDIN_FILENO)) {
        tcgetattr(STDIN_FILENO, &term_settings);
        atexit((void(*)(void))reset_input_mode);
    } 
}

/* --------------------------------------------------------------------------------*/

static PyObject *GTMException;

/*--------------------------------------------------------------------------------*/
/* INOUT is a marker class which flag a parameter as being output                 */

typedef struct {
    PyObject_HEAD
    PyObject *value;
} INOUT;

static void
INOUT_dealloc(INOUT* self)
{
    if (self->value)
        Py_XDECREF(self->value);
    self->ob_type->tp_free((PyObject*)self);
}

static PyObject *
INOUT_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    INOUT *self;

    self = (INOUT *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->value = PyString_FromString("");
        if (self->value == NULL)
        {
            Py_DECREF(self);
            return NULL;
        }
        return (PyObject *)self;
    }
    return NULL;
}

static int
INOUT_init(INOUT *self, PyObject *args, PyObject *kwds)
{
    PyObject *value=NULL, *tmp=NULL;

    if (!PyArg_ParseTuple(args, "O:init", &value))
        return -1;

    if (value) {
        tmp = self->value;
        Py_INCREF(value);
        self->value = value;
        Py_XDECREF(tmp);
    }

    return 0;
}


static PyMethodDef INOUT_methods[] = {
    {NULL}  /* Sentinel */
};

static PyMemberDef INOUT_members[] = {
    {"value", T_OBJECT_EX, offsetof(INOUT, value), 0, "value"},
    {NULL}  /* Sentinel */
};

static PyTypeObject INOUT_type = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "_gtm.INOUT",             /*tp_name*/
    sizeof(INOUT), /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)INOUT_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "INOUT flags a parameter as being returned",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    INOUT_methods,             /* tp_methods */
    INOUT_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)INOUT_init,      /* tp_init */
    0,                         /* tp_alloc */
    INOUT_new,                 /* tp_new */
};

/*--------------------------------------------------------------------------------*/

void mstop(void) {
    if (gInitialised) {
        gInitialised = 0;
        gtm_exit();
        reset_input_mode();
    } else {
        status = 0;
    }
}

static void *
mstart(void) {
    /* TODO: Should I check the lib-path? */
    if (gInitialised) {
        status = 0;
    } else {
        if (!getenv("GTMCI")) {
            PyErr_SetString(GTMException, "GTMCI environment variable not set.");
            return NULL;
        }
        save_input_mode();
        CALLGTM(gtm_init());
        gInitialised = 1;
        atexit(mstop);
    }
    return (void *)1;
}

/* --------------------------------------------------------------------------------*/
/* 
    mexec

    This is a generic shim. It executes a mumps command, and handles input and 
    output parameters. The parameters are typed, so only string (non-unicode),
    long and double parameters are accepted.

    To flag a parameter as a return value, mark it with a marker class.

    The variables are number left to right by their class, i.e. l0 is the first long,
    s0 is the first string and d0 is the first double.

    >>> from vavista._gtm import mexec, INOUT
    >>> print mexec("s l2=l0*l1", 2, 10, INOUT(0))
    (20,)

*/
static void
free_pointers(char *s[]){
    /* Clean up malloc'ed storage used by GTM_mexec */
    int i=0;
    for (i=0; i<8; i++){
        if (s[i]) {free(s[i]);}
    }
}

static PyObject *GTM_mexec(PyObject *self, PyObject *args) {
    int i, i2;
    char eval[MAXVAL];
    int nextInt=0, nextString=0, nextDouble=0;

    char *s[8], *alloced[8], sRv[MAXVAL];
    long l[8], lRv=0;
    double d[8], dRv=0;

    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    struct timespec timeStart, timeEnd; 
    long msec;

    char empty[1] = {'\0'};

    PyObject *rv = NULL;
    int rvCount = 0;

    /* Track return values */
    struct {
        char field_type; /* 's' = string, 'd' = double, 'l' = long, '\0' unused */
        int m_fieldid;   /* index of field 0-7 in mumps parameter list */
    } rv_info[24];

    // Initialse calling variables
    PyObject *py_eval = NULL, *a[24];
    for (i = 0; i< 8; i++) {
        s[i] = empty;
        alloced[i] = NULL;
        l[i] = 0L;
        d[i] = 0.0;
    }
    for (i=0; i<24; i++) a[i] = NULL;
    memset(rv_info, '\0', 24);

    /* This is a static buffer - used by the gtm interface */
    cmd_s.address = "mexec"; cmd_s.length = sizeof(cmd_s.address)-1; cmd.rtn_name=cmd_s; 

    /* returns 1 for success, 0 for failure */
    if (!PyArg_UnpackTuple(args, "mexec", 1, 25, &py_eval,
        &a[0], &a[1], &a[2], &a[3], &a[4], &a[5], &a[6], &a[7], &a[8], &a[9], 
        &a[10], &a[11], &a[12], &a[13], &a[14], &a[15], &a[16], &a[17], &a[18], &a[19], 
        &a[20], &a[21], &a[22], &a[23]))
        return NULL;

    if (!gInitialised) {
        if (mstart() == NULL)
            return NULL;
    }

    if (!PyString_AsString(py_eval)) {
        sprintf(msgbuf, "command parameter must be a string");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    sprintf(eval, "%.*s", MAXVAL-1, PyString_AsString(py_eval));


    /* At this stage we have the parameters as pyobjects.
        If we are to use the value, we need to copy it to an appropriate variable
    */
    for (i=0; i< 24; i++){
        if (a[i]){
            PyObject *value = a[i];
            int inout = 0;

            if (PyObject_TypeCheck(value, &INOUT_type)) {
                INOUT *ob = (INOUT *)value;
                inout = 1;
                value = ob->value;
                rvCount += 1;
            }

            if (PyInt_Check(value)) {
                assert (nextInt < 8);
                l[nextInt] = PyInt_AsLong(value);
                if (inout) {
                    rv_info[i].field_type = 'l';
                    rv_info[i].m_fieldid = nextInt;
                }
                nextInt++;
            }
            else if (PyLong_Check(value)) {
                assert (nextInt < 8);
                l[nextInt] = PyLong_AsLong(value);
                if (inout) {
                    rv_info[i].field_type = 'l';
                    rv_info[i].m_fieldid = nextInt;
                }
                nextInt++;
            }
            else if (PyFloat_Check(value)) {
                assert (nextDouble < 8);
                d[nextDouble] = PyFloat_AsDouble(value);
                if (inout) {
                    rv_info[i].field_type = 'd';
                    rv_info[i].m_fieldid = nextDouble;
                }
                nextDouble++;
            }
            else if (PyString_Check(value) || PyUnicode_Check(value)) {
                assert(nextString < 8);
                alloced[nextString] = s[nextString] = malloc(MAXVAL);
                if (s[nextString] == NULL) {
                    sprintf(msgbuf, "mexec: memory allocation failed");
                    PyErr_SetString(GTMException, msgbuf);
                    free_pointers(alloced);
                    return NULL;
                }
                sprintf(s[nextString], "%.*s", MAXVAL-1, PyString_AsString(value));
                s[nextString][MAXVAL-1] = '\0';
                if (inout) {
                    rv_info[i].field_type = 's';
                    rv_info[i].m_fieldid = nextString;
                }
                nextString++;
            } else {
                sprintf(msgbuf, "mexec: Unable to process parameter %d", i);
                PyErr_SetString(GTMException, msgbuf);
                free_pointers(alloced);
                return NULL;
            }
        } else {
            rv_info[i].field_type = '\0';
        }
    }

    clock_gettime(CLOCK_REALTIME, &timeStart); 

    /* CALL GT.M */
    status = gtm_cip(&cmd, eval,
        s[0],s[1],s[2],s[3],s[4],s[5],s[6],s[7],
        &l[0],&l[1],&l[2],&l[3],&l[4],&l[5],&l[6],&l[7],
        &d[0],&d[1],&d[2],&d[3],&d[4],&d[5],&d[6],&d[7],
        &sRv,&lRv,&dRv);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        free_pointers(alloced);
        return NULL;
    }

    clock_gettime(CLOCK_REALTIME, &timeEnd); 
    msec = timeEnd.tv_nsec - timeStart.tv_nsec;
    msec += (timeEnd.tv_sec - timeStart.tv_sec) * 1000000000;
    msec /= 1000000; 
    /* log time in ms */
    fpLog = fopen("/tmp/log", "a+");
    fprintf(fpLog, "%ld: %s\n", msec, eval);
    fclose(fpLog);

    if (rvCount > 0) {
        rv = PyTuple_New(rvCount);
        for (i=0, i2=0; i< 24; i++){
            if (rv_info[i].field_type == 's') {
                PyTuple_SetItem(rv, i2++, PyString_FromString(s[rv_info[i].m_fieldid]));
            }
            if (rv_info[i].field_type == 'l') {
                PyTuple_SetItem(rv, i2++, PyInt_FromLong(l[rv_info[i].m_fieldid]));
            }
            if (rv_info[i].field_type == 'd') {
                PyTuple_SetItem(rv, i2++, PyFloat_FromDouble(d[rv_info[i].m_fieldid]));
            }
        }
        free_pointers(alloced);
        return rv;
    } else {
        free_pointers(alloced);
        Py_INCREF(Py_None);
        return Py_None;
    }
}


static PyObject*
GTM_tstart(PyObject *self, PyObject *args)
{
#if defined(GTMTX_AVAILABLE)
    char *s;
    long lRv;

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "s", &s)) {
            s = NULL;
    }
    lRv = gtm_txstart(s);
    return Py_BuildValue("l", lRv);

#else
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;

    if (mstart() == NULL) return NULL;

    cmd_s.address = "tstart";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    status = gtm_cip(&cmd);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
#endif

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject*
GTM_tcommit(PyObject *self, PyObject *noarg)
{
#if defined(GTMTX_AVAILABLE)
    xc_status_t	status;

    if (mstart() == NULL) return NULL;

    status = gtm_txcommit();
    if (0 != status ) { 
        sprintf(msgbuf, "GT.M Transaction Commit Failed: Error Code %d", status);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
#else
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;

    if (mstart() == NULL) return NULL;

    cmd_s.address = "tcommit";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    status = gtm_cip(&cmd);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
#endif

    Py_INCREF(Py_None);
    return Py_None;
}


static PyObject*
GTM_trollback(PyObject *self, PyObject *noarg)
{
#if defined(GTMTX_AVAILABLE)
    if (mstart() == NULL) return NULL;

    gtm_txrollback(0);
#else
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;

    if (mstart() == NULL) return NULL;

    cmd_s.address = "trollback";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    status = gtm_cip(&cmd);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
#endif

    Py_INCREF(Py_None);
    return Py_None;
}

/*
 * This is a quick way to return a global. haven't identified
 * how to return a non-persistent global.
 */
static PyObject*
GTM_mget(PyObject *self, PyObject *args)
{
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    char *varname;
    char p[MAXVAL];

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "s", &varname)) {
        sprintf(msgbuf, "mget requires a mumps variable name");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }

    cmd_s.address = "mget";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    status = gtm_cip(&cmd, varname, p);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    return Py_BuildValue("s", p);
}

/*
 * Set a global variable
 */
static PyObject*
GTM_mset(PyObject *self, PyObject *args)
{
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    char *varname, *value;

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "ss", &varname, &value)) {
        sprintf(msgbuf, "mset requires a mumps variable name and a value");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }

    cmd_s.address = "mset";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    status = gtm_cip(&cmd, varname, value);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

/*
 * Order through a global variable
 */
static PyObject*
GTM_mord(PyObject *self, PyObject *args)
{
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    char *varname;
    char p[MAXVAL];

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "s", &varname)) {
        sprintf(msgbuf, "mord requires a mumps variable name");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }

    cmd_s.address = "mord";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    status = gtm_cip(&cmd, varname, p);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    return Py_BuildValue("s", p);
}

/*
 * Order through a data-dictionary record
 *
 * set s0=$order(^DD(s2,s0)) Q:s0'=+s0  s s1=$G(^DD(s2,s0,0)),s3=$G(^DD(s2,s0,.1)),s4=$G(^DD(s2,s0,3))
 */
static PyObject*
GTM_ddwalk(PyObject *self, PyObject *args)
{
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    char *fileid, *fieldid;
    char fieldid_arg[MAXVAL];
    char info[MAXVAL];
    char title[MAXVAL];
    char help[MAXVAL];

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "ss", &fileid, &fieldid)) {
        sprintf(msgbuf, "ddwalk requires a mumps fileid and the current field number");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }

    cmd_s.address = "ddwalk";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    strcpy(fieldid_arg, fieldid); /* mumps overwrites this so need a copy */

    status = gtm_cip(&cmd, fieldid_arg, info, fileid, title, help);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    return Py_BuildValue("ssss", fieldid_arg, info, title, help);
}

/*
 * Order through a gl item. Return the next key, data, value
 *
 * set s0=$order(^DD(s2,s0)) Q:s0'=+s0  s s1=$G(^DD(s2,s0,0)),s3=$G(^DD(s2,s0,.1)),s4=$G(^DD(s2,s0,3))
 */
static PyObject*
GTM_glwalk(PyObject *self, PyObject *args)
{
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    char *global_ref, *key;
    char key_arg[MAXVAL];
    long data;
    char value[MAXVAL];

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "ss", &global_ref, &key)) {
        sprintf(msgbuf, "glwalk requires a mumps global reference and the key");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }

    cmd_s.address = "glwalk";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    strcpy(key_arg, key); /* mumps overwrites this so need a copy */

    status = gtm_cip(&cmd, global_ref, key_arg, &data, value);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    return Py_BuildValue("sls", key_arg, data, value);
}

/*
 * Order through a gl item. Return the next key, data, value(0)
 *
 * This is used for wp fields
 */
static PyObject*
GTM_wpwalk(PyObject *self, PyObject *args)
{
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    char *global_ref, *key;
    char key_arg[MAXVAL];
    long data;
    char value[MAXVAL];

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "ss", &global_ref, &key)) {
        sprintf(msgbuf, "wpwalk requires a mumps global reference and the key");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }

    cmd_s.address = "wpwalk";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    strcpy(key_arg, key); /* mumps overwrites this so need a copy */

    status = gtm_cip(&cmd, global_ref, key_arg, &data, value);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    return Py_BuildValue("sls", key_arg, data, value);
}


static PyObject*
GTM_mkill(PyObject *self, PyObject *args)
{
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    char *varname;

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "s", &varname)) {
        sprintf(msgbuf, "mkill requires a mumps variable name");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }

    cmd_s.address = "mkill";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    status = gtm_cip(&cmd, varname);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject*
GTM_mdata(PyObject *self, PyObject *args)
{
    static ci_name_descriptor cmd;
    static gtm_string_t cmd_s;
    char *varname;
    long data;

    if (mstart() == NULL) return NULL;

    if (!PyArg_ParseTuple(args, "s", &varname)) {
        sprintf(msgbuf, "mdata requires a mumps variable name");
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }

    cmd_s.address = "mdata";
    cmd_s.length = sizeof(cmd_s.address)-1;
    cmd.rtn_name=cmd_s; 

    status = gtm_cip(&cmd, varname, &data);

    if (0 != status ) { 
        gtm_zstatus(msgbuf, MAXMSG);
        PyErr_SetString(GTMException, msgbuf);
        return NULL;
    }
    return Py_BuildValue("l", data);
}


static PyMethodDef GTMMethods[] = {
    {"mexec",   GTM_mexec,   METH_VARARGS, "Dynamically Invoke a Mumps Command."},
    {"mget",   GTM_mget,   METH_VARARGS, "Get a mumps variable."},
    {"mset",   GTM_mset,   METH_VARARGS, "Set a mumps variable."},
    {"mord",   GTM_mord,   METH_VARARGS, "Order through a mumps variable."},
    {"ddwalk",   GTM_ddwalk,   METH_VARARGS, "Order through a data dictionary global."},
    {"mdata",   GTM_mdata,   METH_VARARGS, "$data on variable."},
    {"mkill",   GTM_mkill,   METH_VARARGS, "kill a variable."},
    {"glwalk",   GTM_glwalk,   METH_VARARGS, "Order through a global."},
    {"wpwalk",   GTM_wpwalk,   METH_VARARGS, "Order through a WP subfile."},
    {"tstart",   GTM_tstart,   METH_VARARGS, "Transaction begin."},
    {"tcommit",   GTM_tcommit,   METH_NOARGS, "Transaction commit."},
    {"trollback",   GTM_trollback,   METH_NOARGS, "Transaction rollback."},
    {NULL,     NULL,         0,            NULL}        /* Sentinel */
};

/*--------------------------------------------------------------------------------*/
PyMODINIT_FUNC
init_gtm(void) {
    PyObject *m;
    m = Py_InitModule("_gtm", GTMMethods);
    GTMException = PyErr_NewException("_gtm.error", NULL, NULL);
    Py_INCREF(GTMException);
    PyModule_AddObject(m, "error", GTMException);

    INOUT_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&INOUT_type) < 0)
        return;

    Py_INCREF(&INOUT_type);
    PyModule_AddObject(m, "INOUT", (PyObject *)&INOUT_type); 
    fpLog = fopen("/tmp/log", "w+");
    fclose(fpLog);
}

