package zim

/*
#cgo pkg-config: libzim
#cgo CXXFLAGS: -std=c++17
#include <stdlib.h>
#include "bridge.h"
*/
import "C"

import (
	"errors"
	"runtime"
	"unsafe"
)

type Archive struct {
	ptr *C.struct_ZimArchive
}

func Open(path string) (*Archive, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var cerr *C.char
	ptr := C.zim_open_archive(cpath, &cerr)
	if ptr == nil {
		defer freeCString(cerr)
		if cerr != nil {
			return nil, errors.New(C.GoString(cerr))
		}
		return nil, errors.New("failed to open zim archive")
	}
	arch := &Archive{ptr: ptr}
	runtime.SetFinalizer(arch, func(a *Archive) {
		a.Close()
	})
	return arch, nil
}

func (a *Archive) Close() error {
	if a == nil || a.ptr == nil {
		return nil
	}
	C.zim_close_archive(a.ptr)
	a.ptr = nil
	runtime.SetFinalizer(a, nil)
	return nil
}

func (a *Archive) EntryCount() uint32 {
	if a == nil || a.ptr == nil {
		return 0
	}
	return uint32(C.zim_archive_entry_count(a.ptr))
}

func (a *Archive) EntryAt(index uint32) (*Entry, error) {
	if a == nil || a.ptr == nil {
		return nil, errors.New("archive closed")
	}
	var cerr *C.char
	ptr := C.zim_archive_entry_at(a.ptr, C.uint(index), &cerr)
	if ptr == nil {
		defer freeCString(cerr)
		if cerr != nil {
			return nil, errors.New(C.GoString(cerr))
		}
		return nil, errors.New("failed to read entry")
	}
	entry := &Entry{ptr: ptr}
	runtime.SetFinalizer(entry, func(e *Entry) {
		e.Close()
	})
	return entry, nil
}

type Entry struct {
	ptr *C.struct_ZimEntry
}

func (e *Entry) Close() {
	if e == nil || e.ptr == nil {
		return
	}
	C.zim_entry_free(e.ptr)
	e.ptr = nil
	runtime.SetFinalizer(e, nil)
}

func (e *Entry) Title() string {
	if e == nil || e.ptr == nil {
		return ""
	}
	cstr := C.zim_entry_get_title(e.ptr)
	defer freeCString(cstr)
	if cstr == nil {
		return ""
	}
	return C.GoString(cstr)
}

func (e *Entry) Path() string {
	if e == nil || e.ptr == nil {
		return ""
	}
	cstr := C.zim_entry_get_path(e.ptr)
	defer freeCString(cstr)
	if cstr == nil {
		return ""
	}
	return C.GoString(cstr)
}

func (e *Entry) MimeType(followRedirect bool) string {
	if e == nil || e.ptr == nil {
		return ""
	}
	follow := C.int(0)
	if followRedirect {
		follow = 1
	}
	cstr := C.zim_entry_get_mimetype(e.ptr, follow)
	defer freeCString(cstr)
	if cstr == nil {
		return ""
	}
	return C.GoString(cstr)
}

func (e *Entry) IsRedirect() bool {
	if e == nil || e.ptr == nil {
		return false
	}
	return C.zim_entry_is_redirect(e.ptr) != 0
}

func (e *Entry) HasItem() bool {
	if e == nil || e.ptr == nil {
		return false
	}
	return C.zim_entry_has_item(e.ptr) != 0
}

func (e *Entry) Data() ([]byte, error) {
	if e == nil || e.ptr == nil {
		return nil, errors.New("entry closed")
	}
	var data unsafe.Pointer
	var size C.size_t
	var cerr *C.char
	rc := C.zim_entry_get_data(e.ptr, &data, &size, &cerr)
	if rc != 0 {
		defer freeCString(cerr)
		if cerr != nil {
			return nil, errors.New(C.GoString(cerr))
		}
		return nil, errors.New("failed to read entry data")
	}
	length := int(size)
	var result []byte
	if length > 0 {
		result = C.GoBytes(data, C.int(length))
	} else {
		result = []byte{}
	}
	C.zim_free(data)
	return result, nil
}

func freeCString(str *C.char) {
	if str != nil {
		C.zim_free(unsafe.Pointer(str))
	}
}
