/*
    This is the gtm compatibility layer. I expect to implement a Cache alternative
    at some stage.

    This code was heavily influenced by pyGTMx.
*/
#include <Python.h>
#include "structmember.h"

#include <string.h>
#include <termios.h>
#include <unistd.h>

#include "gtmxc_types.h"

#define MAXMSG 2048      /* max length of a GT.M message */
#define MAXVAL 1048576  /* max length of a value GT.M can return */

static gtm_char_t     msgbuf[MAXMSG];
static gtm_status_t   status;
static int gInitialised=0;

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

    if (!PyString_Check(py_eval)) {
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
            else if (PyString_Check(value)) {
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

static PyMethodDef GTMMethods[] = {
    {"mexec",   GTM_mexec,   METH_VARARGS, "Dynamically Invoke a Mumps Command."},
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
}

