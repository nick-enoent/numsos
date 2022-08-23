CFLAGS := $(filter-out -Werror, ${CFLAGS})
PY_INCLUDES = -I$(srcdir)/../include \
	   -I$(srcdir)/../src \
	   -I$(top_srcdir)/ods/include \
	   -I$(NUMPY_INCLUDE_PATH)

pkgpythondir=${pythondir}/numsos
pkgpython_PYTHON = __init__.py \
	Csv.py \
	DataSource.py \
	Stack.py \
	Transform.py \
	ArgParse.py

pkgpyexec_LTLIBRARIES = Inputer.la

Inputer_la_SOURCES = Inputer.pyx
nodist_Inputer_la_SOURCES = Inputer.c
Inputer_la_CFLAGS = -g -O0  $(PY_INCLUDES) $(PYTHON_CPPFLAGS)
Inputer_la_LDFLAGS = $(PYTHON_LDFLAGS) -module -shared
Inputer_la_LIBADD = @SOS_LIBDIR_FLAG@ @SOS_LIB64DIR_FLAG@ -lsos

pkgpyexecdir = $(pkgpythondir)

CYTHON_GDB = $(shell [ "$(uname -p)" = "x86_64" ] && echo --gdb || echo "" )

Inputer.c: Inputer.pyx
	echo PYTHON_LDFLAGS are "$(PYTHON_LDFLAGS)"
	cython -3 --directive language_level=3 $(CYTHON_GDB) $< -o $@

