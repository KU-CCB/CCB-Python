CXX=g++

CXXDEFINES=-DHAVE_LIBZ -DHAVE_SHARED_POINTER -DHAVE_WXWIDGETS -DUSING_DYNAMIC_LIBS -DWXUSINGDLL -D_FILE_OFFSET_BITS=64 -D__WXGTK__ -Dplugin_charges_EXPORTS

LDLIBS=-lm -ldl -lz -lopenbabel

# ITTC db2, acf cluster
#CXXFLAGS=-g -pthread -O3 -DNDEBUG -O2 -DNDEBUG -fPIC -isystem /tools/cluster/6.2/openbabel/2.3.2/lib/wx/include/gtk2-unicode-3.0 -isystem /tools/cluster/6.2/openbabel/2.3.2/include/wx-3.0 -I/tools/cluster/system/pkg/openbabel/openbabel-2.3.2/build/include -I/tools/cluster/system/pkg/openbabel/openbabel-2.3.2/data -I/tools/cluster/system/pkg/openbabel/openbabel-2.3.2/include -I/tools/cluster/6.2/openbabel/2.3.2/include/openbabel-2.0 -DOB_MODULE_PATH="\"/tools/cluster/6.2/openbabel/2.3.2/lib/openbabel/2.3.2\"" /tools/cluster/6.2/openbabel/2.3.2/lib/libopenbabel.so.4.0.2
#LDFLAGS=-Wl,-rpath,/tools/cluster/6.2/openbabel/2.3.2/lib:/tools/cluster/system/pkg/openbabel/openbabel-2.3.2/build/lib,-L/tools/cluster/6.2/openbabel/2.3.2/lib
# Local machine
CXXFLAGS=-g -pthread -O3 -DNDEBUG -O2 -DNDEBUG -fPIC -isystem -isystem -I/usr/local/include -I/usr/local/include -DOB_MODULE_PATH="\"/usr/local/lib/\"" /usr/local/lib/libopenbabel.so.4.0.2
LDFLAGS=-Wl,-rpath,/usr/local/lib,-L/usr/local/lib

xattr: 
	$(CXX) $(CXXDEFINES) $(CXXFLAGS) -o xattr xattr.cpp $(LDFLAGS) $(LDLIBS)

strgt:
	$(CXX) -o strgt strgt.cc	

clean:
	@rm -f xattr
