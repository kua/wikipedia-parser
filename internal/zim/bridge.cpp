#include "bridge.h"

#include <zim/archive.h>
#include <zim/entry.h>
#include <zim/item.h>
#include <zim/blob.h>
#include <zim/error.h>

#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <string>

namespace {

struct ArchiveWrapper {
    zim::Archive* archive;
};

struct EntryWrapper {
    zim::Entry* entry;
};

char* dupString(const std::string& value) {
    auto size = value.size();
    char* out = static_cast<char*>(std::malloc(size + 1));
    if (!out) {
        return nullptr;
    }
    if (size > 0) {
        std::memcpy(out, value.data(), size);
    }
    out[size] = '\0';
    return out;
}

void assignError(const std::exception& ex, char** err) {
    if (err) {
        *err = dupString(ex.what());
    }
}

void assignUnknownError(char** err) {
    if (err) {
        *err = dupString("unknown libzim error");
    }
}

}  // namespace

extern "C" {

struct ZimArchive* zim_open_archive(const char* path, char** err) {
    if (!path) {
        if (err) {
            *err = dupString("invalid path");
        }
        return nullptr;
    }
    try {
        auto wrapper = new ArchiveWrapper();
        wrapper->archive = new zim::Archive(std::string(path));
        return reinterpret_cast<ZimArchive*>(wrapper);
    } catch (const std::exception& ex) {
        assignError(ex, err);
    } catch (...) {
        assignUnknownError(err);
    }
    return nullptr;
}

void zim_close_archive(struct ZimArchive* archive) {
    if (!archive) {
        return;
    }
    auto wrapper = reinterpret_cast<ArchiveWrapper*>(archive);
    delete wrapper->archive;
    wrapper->archive = nullptr;
    delete wrapper;
}

unsigned int zim_archive_entry_count(struct ZimArchive* archive) {
    if (!archive) {
        return 0;
    }
    auto wrapper = reinterpret_cast<ArchiveWrapper*>(archive);
    try {
        auto count = wrapper->archive->getEntryCount();
        if (count < 0) {
            return 0;
        }
        return static_cast<unsigned int>(count);
    } catch (...) {
        return 0;
    }
}

struct ZimEntry* zim_archive_entry_at(struct ZimArchive* archive, unsigned int index, char** err) {
    if (!archive) {
        if (err) {
            *err = dupString("archive closed");
        }
        return nullptr;
    }
    auto wrapper = reinterpret_cast<ArchiveWrapper*>(archive);
    try {
        auto entry = wrapper->archive->getEntryByPath(index);
        auto holder = new EntryWrapper();
        holder->entry = new zim::Entry(entry);
        return reinterpret_cast<ZimEntry*>(holder);
    } catch (const std::exception& ex) {
        assignError(ex, err);
    } catch (...) {
        assignUnknownError(err);
    }
    return nullptr;
}

void zim_entry_free(struct ZimEntry* entry) {
    if (!entry) {
        return;
    }
    auto wrapper = reinterpret_cast<EntryWrapper*>(entry);
    delete wrapper->entry;
    wrapper->entry = nullptr;
    delete wrapper;
}

char* zim_entry_get_title(struct ZimEntry* entry) {
    if (!entry) {
        return nullptr;
    }
    auto wrapper = reinterpret_cast<EntryWrapper*>(entry);
    try {
        return dupString(wrapper->entry->getTitle());
    } catch (...) {
        return nullptr;
    }
}

char* zim_entry_get_path(struct ZimEntry* entry) {
    if (!entry) {
        return nullptr;
    }
    auto wrapper = reinterpret_cast<EntryWrapper*>(entry);
    try {
        return dupString(wrapper->entry->getPath());
    } catch (...) {
        return nullptr;
    }
}

char* zim_entry_get_mimetype(struct ZimEntry* entry, int follow_redirect) {
    if (!entry) {
        return nullptr;
    }
    auto wrapper = reinterpret_cast<EntryWrapper*>(entry);
    try {
        zim::Item item = wrapper->entry->getItem(follow_redirect != 0);
        return dupString(item.getMimetype());
    } catch (...) {
        return nullptr;
    }
}

int zim_entry_is_redirect(struct ZimEntry* entry) {
    if (!entry) {
        return 0;
    }
    auto wrapper = reinterpret_cast<EntryWrapper*>(entry);
    try {
        return wrapper->entry->isRedirect() ? 1 : 0;
    } catch (...) {
        return 0;
    }
}

int zim_entry_has_item(struct ZimEntry* entry) {
    if (!entry) {
        return 0;
    }
    auto wrapper = reinterpret_cast<EntryWrapper*>(entry);
    try {
        wrapper->entry->getItem(false);
        return 1;
    } catch (...) {
        return 0;
    }
}

int zim_entry_get_data(struct ZimEntry* entry, void** data, size_t* size, char** err) {
    if (!entry || !data || !size) {
        if (err) {
            *err = dupString("invalid arguments");
        }
        return -1;
    }
    auto wrapper = reinterpret_cast<EntryWrapper*>(entry);
    try {
        zim::Item item = wrapper->entry->getItem(false);
        zim::Blob blob = item.getData();
        auto blobSize = blob.size();
        void* buffer = nullptr;
        if (blobSize > 0) {
            buffer = std::malloc(blobSize);
            if (!buffer) {
                if (err) {
                    *err = dupString("allocation failure");
                }
                return -1;
            }
            std::memcpy(buffer, blob.data(), blobSize);
        }
        *data = buffer;
        *size = blobSize;
        return 0;
    } catch (const std::exception& ex) {
        assignError(ex, err);
    } catch (...) {
        assignUnknownError(err);
    }
    return -1;
}

void zim_free(void* ptr) {
    std::free(ptr);
}

}  // extern "C"
