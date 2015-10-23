#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>

#define BS (1024*1024)

struct binfile {
	int fd;
	struct buff *b1;
	struct buff *b2;
	struct buff *cur_buff;
	off_t  cur_off;
	off_t  cur_nr;
};

struct buff {
	unsigned char b[BS];
	size_t sz;
	int nr;
	int last;
};

static inline void buff_switch(struct binfile *bf)
{
	bf->cur_buff = (bf->cur_buff == bf->b1)? bf->b2: bf->b1;
}

static inline struct buff *buff_cur(struct binfile *bf)
{
	return bf->cur_buff;
}

int buff_next(struct binfile *bf)
{
	ssize_t rc;
	struct buff *b;
	buff_switch(bf);
	bf->cur_nr ++;
	b = buff_cur(bf);
	if (b->nr < bf->cur_nr) {
		rc = read(bf->fd, b->b, BS);
		if (rc <= 0) {
			bf->cur_nr --;
			buff_switch(bf);
			return -1;
		}
		if (rc < BS)
			b->last = 1;
		b->sz = (size_t)rc;
		b->nr = bf->cur_nr;
	}
	return 0;
}

int buff_dump(struct binfile *bf)
{
	int i;
	unsigned char *p = bf->cur_buff->b + bf->cur_off;
	int size = bf->cur_buff->sz - bf->cur_off; 
	size = (size > 64)?64:size;
	for (i = 0; i < size; i ++)
		fprintf(stdout, "%c", p[i]);
	fprintf(stdout, "...\n");
	return 0;
}
int buff_get(struct binfile *bf, int off,  unsigned char *out, size_t size)
{
	int l1;
	struct buff *b;
	bf->cur_off += off;
	if (bf->cur_off < 0) { /* backward */
		if (!bf->cur_nr)
			return -1;
		bf->cur_nr --;
		buff_switch(bf);
		if (bf->cur_buff->nr != bf->cur_nr) {
			return -1;
		}
		bf->cur_off += bf->cur_buff->sz;
	}
	b = buff_cur(bf);
	if (bf->cur_off >= b->sz) { /* next buffer */
		bf->cur_off -= b->sz;
		if (buff_next(bf)) {
			bf->cur_off += b->sz;
			return -1;
		}
		b = buff_cur(bf);
	}
	/* here we go */
	l1 = b->sz - bf->cur_off;
	if (l1 >= size) {
		memcpy(out, b->b + bf->cur_off, size);
		return 0;
	}
	memcpy(out, b->b + bf->cur_off, l1);
	out += l1;
	if (b->last) {
	    return l1;
	}
	if (buff_next(bf))
		return -1;
	b = buff_cur(bf);
	memcpy(out, b->b, size - l1);
	buff_switch(bf); /* returned back */
	bf->cur_nr --;
	return 0;
}

struct binfile *buff_init(const char *fname)
{
	struct binfile *b;
	b = malloc(sizeof(struct binfile));
	if (!b)
		return NULL;
	b->fd = open(fname, O_RDONLY);
	if (b->fd < 0)
		goto err;
	b->b1 = malloc(sizeof(struct buff));
	b->b2 = malloc(sizeof(struct buff));
	if (!b->b1 || !b->b2)
		goto err1;
	b->b1->nr = b->b2->nr = -1;
	b->b1->last = b->b2->last = 0;
	b->cur_buff = b->b1;
	b->cur_off = 0;
	b->cur_nr = -1;
	return b;
err1:
	if (b->b1)
		free(b->b1);
	if (b->b2)
		free(b->b2);
err:
	free(b);
	return NULL;
}
static int pmap[256];
static int pmap_full[256];
static int pmap_full_size = 0;
static int pmap_size = 0;
static int pmap_pos = 0;

int sp_init(const unsigned char *buf, size_t size)
{
	int i, k;
	int cur_size = 0;
	int cur_pos = 0;

	for (k = 0; k < 256; k++)
		pmap_full[k] = -1;

	for (i = 0; i < size; i ++) {
		if (pmap_full[buf[i]] == -1)
			pmap_full[buf[i]] = i;
		else
			pmap_full[buf[i]] = -2; /* not unique */
	}
	pmap_full_size = size;
	for (i = 0; i < size; i ++) {
		if (pmap_size >= size - i)
			goto out;
		for (k = 0; k < 256; k++)
			pmap[k] = -1;
		cur_size = 0;
		cur_pos = i;
		for (k = i; k < size; k ++) {
			unsigned char c = buf[k];
			if (pmap[c] != -1)
				break;
			pmap[c] = k;
			cur_size ++;
		}
		if (cur_size > pmap_size) {
			pmap_size = cur_size;
			pmap_pos = cur_pos;
		}
	}
out:
	for (k = 0; k < 256; k++)
		pmap[k] = -1;
	for (k = 0; k < pmap_size; k ++) {
		pmap[buf[k + pmap_pos]] = k + pmap_pos;
	}
	return 0;
}
	static off_t last_offset = -1;
	static time_t tm = 0;
	static time_t ftime = 0;
	static int thr = 0;

static inline int stats(off_t offset)
{
	off_t kb, mb, gb, speed;
	thr ++;
	if (thr % 1000)
		return 0;
	if (!ftime)
		ftime = time(NULL);

	if (last_offset == -1)
		last_offset = offset;

	if (time(NULL) <= tm || time(NULL) == ftime)
		return 0;

	kb = offset / 1024;
	mb = kb / 1024;
	gb = mb / 1024;

	tm = time(NULL);
	speed = (offset) / 1024 / 1024 / (tm - ftime);
	if (gb) {
		fprintf(stderr,"\r%ld.%ldGb @ %ldMb/s...", gb, mb % 1024, speed);
	} else if (mb) {
		fprintf(stderr,"\r%ldMb @ %ldMb/s...", mb, speed);
	} else if (kb) {
		fprintf(stderr,"\r%ldKb @ %ldMb/s...", kb, speed);
	}
	last_offset = offset;
	return 0;
}

int main(int argc, char **argv)
{
	off_t last_offset;

	struct binfile *bf;
	off_t offset = 0;
	unsigned char *chunk;
	int str_len = 0;
	if (argc < 3) {
		fprintf(stderr, "Usage: %s <fname> <pattern>\n", argv[0]);
		return 1;
	}
	bf = buff_init(argv[1]);
	if (!bf) {
		fprintf(stderr, "Can not open file?\n");
		return 1;
	}
	str_len = strlen(argv[2]);

	if (sp_init((const unsigned char*)argv[2], str_len)) {
		fprintf(stderr, "Can not init pattern?\n");
		return 1;
	}
	chunk = malloc(str_len + 1);
	if (!chunk) {
		fprintf(stderr, "Can not alloc  pattern?\n");
		return 1;
	}
	if (buff_get(bf, pmap_pos, chunk, str_len)) {
		fprintf(stderr, "Can init chunk?\n");
		return 1;		
	}
	offset += pmap_pos;
	last_offset = offset; 
	while(1) {
		int rc;
		stats(offset);
		int off = pmap[chunk[0]];
		if (off != -1 && off <= offset) {
			offset -= off;
			if (buff_get(bf, -off, chunk, str_len)) {
				fprintf(stderr, "Cant rewind!\n");
				return 1;
			}
			if (!memcmp(argv[2], chunk, str_len)) {
				fprintf(stdout, "O:%ld\n", offset);
				buff_dump(bf);
			}
		} else
			off = 0;
		while (1) { /* fast path */
			offset += (pmap_full_size + off);
			rc = buff_get(bf, pmap_full_size + off, chunk, str_len);
			if (rc < 0) {
				fprintf(stderr, "Cant next buff\n");
				return 1;
			}
			off = 0;
			if (pmap_full[chunk[0]] == -1)
				continue;
			if (pmap_full[chunk[0]] == -2) { 
				offset += -(pmap_full_size) + pmap_size;
				rc = buff_get(bf, -(pmap_full_size) + pmap_size, chunk, str_len);
				if (rc < 0) {
					fprintf(stderr, "Cant rewind\n");
					return 1;
				}
				break;
			} /* try match */
			off = pmap_full[chunk[0]];
			rc = buff_get(bf, -off, chunk, str_len);
			if (rc < 0) {
				fprintf(stderr, "Cant rewind\n");
				return 1;
			}
			offset -= off;
			if (!memcmp(argv[2], chunk, str_len)) {
				fprintf(stdout, "O:%ld\n", offset);
				buff_dump(bf);
				off = 0;
				// return 0;
			}
		}
	}
	return 1;
}
