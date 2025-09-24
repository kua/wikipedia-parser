#ifndef GO_LIBZIM_BRIDGE_H
#define GO_LIBZIM_BRIDGE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ZimArchive;
struct ZimEntry;

struct ZimArchive* zim_open_archive(const char* path, char** err);
void zim_close_archive(struct ZimArchive* archive);
unsigned int zim_archive_entry_count(struct ZimArchive* archive);
struct ZimEntry* zim_archive_entry_at(struct ZimArchive* archive, unsigned int index, char** err);
void zim_entry_free(struct ZimEntry* entry);

char* zim_entry_get_title(struct ZimEntry* entry);
char* zim_entry_get_path(struct ZimEntry* entry);
char* zim_entry_get_mimetype(struct ZimEntry* entry, int follow_redirect);
int zim_entry_is_redirect(struct ZimEntry* entry);
int zim_entry_has_item(struct ZimEntry* entry);

int zim_entry_get_data(struct ZimEntry* entry, void** data, size_t* size, char** err);

void zim_free(void* ptr);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // GO_LIBZIM_BRIDGE_H
