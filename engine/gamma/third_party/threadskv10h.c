// btree version threadskv10h futex version
//	with reworked bt_deletekey code,
//	phase-fair re-entrant reader writer lock,
//	librarian page split code,
//	duplicate key management
//	bi-directional cursors
//	ACID batched key-value updates
//	LSM B-trees for write optimization
//	larger sized leaf pages than non-leaf
//	and LSM B-tree find & count operations

// 15 DEC 2014

// author: karl malbrain, malbrain@cal.berkeley.edu

/*
This work, including the source code, documentation
and related data, is placed into the public domain.

The orginal author is Karl Malbrain.

THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
RESULTING FROM THE USE, MODIFICATION, OR
REDISTRIBUTION OF THIS SOFTWARE.
*/

// Please see the project home page for documentation
// code.google.com/p/high-concurrency-btree

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE

#ifdef linux
#define _GNU_SOURCE
#include <xmmintrin.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#endif

#ifdef unix
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>
#include <pthread.h>
#include <limits.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <process.h>
#include <intrin.h>
#endif

#include <memory.h>
#include <string.h>
#include <stddef.h>

typedef unsigned long long	uid;
typedef unsigned long long	logseqno;

#ifndef unix
typedef unsigned long long	off64_t;
typedef unsigned short		ushort;
typedef unsigned int		uint;
#endif

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw

#define BT_maxbits		26					// maximum page size in bits
#define BT_minbits		9					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size
#define BT_maxpage		(1 << BT_maxbits)	// maximum page size

//  BTree page number constants
#define ALLOC_page		0	// allocation page
#define ROOT_page		1	// root of the btree
#define LATCH_page		2	// first page of latches

#define SEG_bits		16	// number of leaf pages in a segment in bits
#define MIN_seg			32	// initial number of mapping segments

//	Number of levels to create in a new BTree
#define MIN_lvl			2

/*
There are six lock types for each node in four independent sets: 
1. (set 1) AccessIntent: Sharable. Going to Read the node. Incompatible with NodeDelete. 
2. (set 1) NodeDelete: Exclusive. About to release the node. Incompatible with AccessIntent. 
3. (set 2) ReadLock: Sharable. Read the node. Incompatible with WriteLock. 
4. (set 2) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and other WriteLocks. 
5. (set 3) ParentModification: Exclusive. Change the node's parent keys. Incompatible with another ParentModification. 
6. (set 4) LinkModification: Exclusive. Update of a node's left link is underway. Incompatible with another LinkModification. 
*/

typedef enum{
	BtLockAccess = 1,
	BtLockDelete = 2,
	BtLockRead   = 4,
	BtLockWrite  = 8,
	BtLockParent = 16,
	BtLockLink   = 32
} BtLock;

typedef struct {
  union {
	struct {
	  volatile unsigned char xcl[1];
	  volatile unsigned char filler;
	  volatile ushort waiters[1];
	} bits[1];
	uint value[1];
  };
} MutexLatch;

//	definition for reader/writer reentrant lock implementation

typedef struct {
  MutexLatch xcl[1];
  MutexLatch wrt[1];
  ushort readers;	// number of readers holding lock
#ifdef DEBUG
  ushort line;		// owner source line number
#endif
  ushort dup;		// re-entrant lock count
  pid_t tid;		// owner pid
} RWLock;

//  hash table entries

typedef struct {
	MutexLatch latch[1];
	uint entry;		// Latch table entry at head of chain
} BtHashEntry;

//	latch manager table structure

typedef struct {
	uid page_no;			// latch set page number
	MutexLatch modify[1];	// modify entry lite latch
	RWLock readwr[1];		// read/write page lock
	RWLock access[1];		// Access Intent/Page delete
	RWLock parent[1];		// Posting of fence key in parent
	RWLock link[1];			// left link update in progress
	uint split;				// right split page atomic insert
	uint next;				// next entry in hash table chain
	uint prev;				// prev entry in hash table chain
	uint pin;				// number of accessing threads
} BtLatchSet;

//	Define the length of the page record numbers

#define BtId 6

//	Page key slot definition.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called. The fence key (highest key) for
//	a leaf page is always present, even after cleanup.

//	Slot types

//	In addition to the Unique keys that occupy slots
//	there are Librarian and Duplicate key
//	slots occupying the key slot array.

//	The Librarian slots are dead keys that
//	serve as filler, available to add new Unique
//	or Dup slots that are inserted into the B-tree.

//	The Duplicate slots have had their key bytes extended
//	by 6 bytes to contain a binary duplicate key uniqueifier.

typedef enum {
	Unique,
	Update,
	Librarian,
	Duplicate,
	Delete
} BtSlotType;

typedef struct {
	uint off:BT_maxbits;	// page offset for key start
	uint type:3;			// type of slot
	uint dead:1;			// set for deleted slot
} BtSlot;

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by the key
//	bytes.

typedef struct {
	unsigned char len;		// this can be changed to a ushort or uint
	unsigned char key[0];
} BtKey;

//	the value structure also occupies space at the upper
//	end of the page. Each key is immediately followed by a value.

typedef struct {
	unsigned char len;		// this can be changed to a ushort or uint
	unsigned char value[0];
} BtVal;

#define BT_maxkey	255		// maximum number of bytes in a key
#define BT_keyarray (BT_maxkey + sizeof(BtKey))

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

typedef struct BtPage_ {
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key/value offset
	uint fence;					// page fence key offset
	uint garbage;				// page garbage in bytes
	unsigned char lvl;			// level of page, zero = leaf
	unsigned char free;			// page is on the free chain
	unsigned char kill;			// page is being deleted
	unsigned char nopromote;	// page is being constructed
	uid right, left;			// page numbers to right and left
} *BtPage;

//  The loadpage interface object

typedef struct {
	BtPage page;		// current page pointer
	BtLatchSet *latch;	// current page latch set
} BtPageSet;

//	structure for latch manager on shared ALLOC_page

typedef struct {
	uid allocpage;					// page number of first available page
	uid freechain;					// head of free page_nos chain
	uid leafchain;					// head of leaf page_nos chain
	uid leaf_page;					// page number of leftmost leaf
	uid rightleaf;					// page number of rightmost leaf
	uid leafpromote;				// next leaf page to try promotion
	unsigned long long leafpages;	// number of active leaf pages
	unsigned long long upperpages;	// number of active upper pages
	unsigned char leaf_xtra;		// leaf page size in xtra bits
	unsigned char page_bits;		// base page size in bits
	uint nlatchpage;				// size of buffer pool & latchsets
	uint latchtotal;				// number of page latch entries
	uint latchvictim;				// next latch entry to test for pin
	uint latchhash;					// number of latch hash table slots
	MutexLatch lock[1];				// allocation area lite latch
	MutexLatch promote[1];			// promotion lite latch
} BtPageZero;

//	The object structure for Btree access

typedef struct {
	uint page_size;				// base page size	
	uint page_bits;				// base page size in bits	
	uint leaf_xtra;				// leaf xtra bits	
#ifdef unix
	int idx;
#else
	HANDLE idx;
#endif
	BtPageZero *pagezero;		// mapped allocation page
	BtHashEntry *hashtable;		// the buffer pool hash table entries
	BtLatchSet *latchsets;		// mapped latch set from buffer pool
	uint maxleaves;				// leaf page count to begin promote
	int err;					// last error
	int line;					// last error line no
	int found;					// number of keys found by delete
	int type;					// type of LSM tree 0=cache, 1=main
	uint maxseg;				// max number of memory mapped segments
	uint segments;				// number of memory mapped segments in use
	MutexLatch maps[1];			// segment table mutex
	unsigned char **pages;		// memory mapped segments of b-tree
} BtMgr;

typedef struct {
	BtMgr *mgr;					// buffer manager for entire process
	BtMgr *main;				// buffer manager for main btree
	pid_t tid;					// thread-id of thread
	BtPageSet cacheset[1];		// cached page frame for cache btree
	BtPageSet mainset[1];		// cached page frame for main btree
	uint cacheslot;				// slot number in cacheset
	uint mainslot;				// slot number in mainset
	ushort phase;				// 1 = main btree 0 = cache btree 2 = both
	BtSlot *cachenode;
	BtSlot *mainnode;
	BtKey *cachekey;
	BtKey *mainkey;
	BtVal *cacheval;
	BtVal *mainval;
} BtDb;

typedef struct {
	uint entry:31;		// latch table entry number
	uint reuse:1;		// reused previous page
	uint slot;			// slot on page
	uint src;			// source slot
} AtomicTxn;

//	Catastrophic errors

typedef enum {
	BTERR_ok = 0,
	BTERR_struct,
	BTERR_ovflw,
	BTERR_lock,
	BTERR_map,
	BTERR_read,
	BTERR_wrt,
	BTERR_atomic
} BTERR;

// B-Tree functions

extern void bt_close (BtDb *bt);
extern BtDb *bt_open (BtMgr *mgr, BtMgr *main);
extern BTERR bt_writepage (BtMgr *mgr, BtPage page, uid page_no, uint leaf);
extern void bt_lockpage(BtLock mode, BtLatchSet *latch, pid_t tid, uint line);
extern void bt_unlockpage(BtLock mode, BtLatchSet *latch, uint line);
extern BTERR bt_insertkey (BtMgr *mgr, unsigned char *key, uint len, uint lvl, void *value, uint vallen, BtSlotType type);
extern BTERR  bt_deletekey (BtMgr *mgr, unsigned char *key, uint len, uint lvl);

extern int bt_findkey (BtDb *db, unsigned char *key, uint keylen, unsigned char *value, uint valmax);

extern BTERR bt_startkey (BtDb *db, unsigned char *key, uint len);
extern BTERR bt_nextkey (BtDb *bt);

extern uint bt_lastkey (BtDb *bt);
extern uint bt_prevkey (BtDb *bt);

//	manager functions
extern BtMgr *bt_mgr (char *name, uint bits, uint leaf_xtra, uint poolsize);
extern void bt_mgrclose (BtMgr *mgr);

//	atomic transaction functions
BTERR bt_atomicexec(BtMgr *mgr, BtPage source, uint count, pid_t tid);
BTERR bt_promote (BtDb *bt);

//  The page is allocated from low and hi ends.
//  The key slots are allocated from the bottom,
//	while the text and value of the key
//  are allocated from the top.  When the two
//  areas meet, the page is split into two.

//  A key consists of a length byte, two bytes of
//  index number (0 - 65535), and up to 253 bytes
//  of key value.

//  Associated with each key is a value byte string
//	containing any value desired.

//  The b-tree root is always located at page 1.
//	The first leaf page of level zero is always
//	located on page 2.

//	The b-tree pages are linked with next
//	pointers to facilitate enumerators,
//	and provide for concurrency.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup. The fence key for a leaf node is
//	always present

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse. It also
//	contains the latch manager hash table.

//	The ParentModification lock on a node is obtained to serialize posting
//	or changing the fence key for a node.

//	Empty pages are chained together through the ALLOC page and reused.

//	Access macros to address slot and key values from the page
//	Page slots use 1 based indexing.

#define slotptr(page, slot) (((BtSlot *)(page+1)) + ((slot)-1))
#define keyptr(page, slot) ((BtKey*)((unsigned char*)(page) + slotptr(page, slot)->off))
#define valptr(page, slot) ((BtVal*)(keyptr(page,slot)->key + keyptr(page,slot)->len))
#define fenceptr(page) ((BtKey*)((unsigned char*)(page) + page->fence))

void bt_putid(unsigned char *dest, uid id)
{
int i = BtId;

	while( i-- )
		dest[i] = (unsigned char)id, id >>= 8;
}

uid bt_getid(unsigned char *src)
{
uid id = 0;
int i;

	for( i = 0; i < BtId; i++ )
		id <<= 8, id |= *src++; 

	return id;
}

//	lite weight spin lock Latch Manager

pid_t sys_gettid ()
{
	return syscall(SYS_gettid);
}

int sys_futex(void *addr1, int op, int val1, struct timespec *timeout, void *addr2, int val3)
{
	return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}

void bt_mutexlock(MutexLatch *latch)
{
uint idx, waited = 0;
MutexLatch prev[1];

 while( 1 ) {
  for( idx = 0; idx < 100; idx++ ) {
	*prev->value = __sync_fetch_and_or (latch->value, 1);
	if( !*prev->bits->xcl ) {
	  if( waited )
		__sync_fetch_and_sub (latch->bits->waiters, 1);
	  return;
	}
  }

  if( !waited ) {
	__sync_fetch_and_add (latch->bits->waiters, 1);
	*prev->bits->waiters += 1;
	waited++;
  }

  sys_futex (latch->value, FUTEX_WAIT, *prev->value, NULL, NULL, 0);
 }
}

int bt_mutextry(MutexLatch *latch)
{
	return !__sync_lock_test_and_set (latch->bits->xcl, 1);
}

void bt_releasemutex(MutexLatch *latch)
{
MutexLatch prev[1];

	*prev->value = __sync_fetch_and_and (latch->value, 0xffff0000);

	if( *prev->bits->waiters )
		sys_futex( latch->value, FUTEX_WAKE, 1, NULL, NULL, 0 );
}

//	reader/writer lock implementation

void WriteLock (RWLock *lock, pid_t tid, uint line)
{
	if( tid && lock->tid == tid ) {
		lock->dup++;
		return;
	}
	bt_mutexlock (lock->xcl);
	bt_mutexlock (lock->wrt);
	bt_releasemutex (lock->xcl);
	lock->tid = tid;
#ifdef DEBUG
	lock->line = line;
#endif
}

void WriteRelease (RWLock *lock)
{
	if( lock->dup ) {
		lock->dup--;
		return;
	}
	lock->tid = 0;
	bt_releasemutex (lock->wrt);
}

void ReadLock (RWLock *lock)
{
	bt_mutexlock (lock->xcl);

	if( !__sync_fetch_and_add (&lock->readers, 1) )
		bt_mutexlock (lock->wrt);

	bt_releasemutex (lock->xcl);
}

void ReadRelease (RWLock *lock)
{
	if( __sync_fetch_and_sub (&lock->readers, 1) == 1 )
		bt_releasemutex (lock->wrt);
}

//	read page into buffer pool from permanent location in Btree file

BTERR bt_readpage (BtMgr *mgr, BtPage page, uid page_no, uint leaf)
{
uint page_size = mgr->page_size;

  if( leaf )
	page_size <<= mgr->leaf_xtra;

  if( pread(mgr->idx, page, page_size, page_no << mgr->page_bits) < page_size )
	return mgr->err = BTERR_read;

  return 0;
}

//	write page to location in Btree file

BTERR bt_writepage (BtMgr *mgr, BtPage page, uid page_no, uint leaf)
{
uint page_size = mgr->page_size;

  if( leaf )
	page_size <<= mgr->leaf_xtra;

  if( pwrite(mgr->idx, page, page_size, page_no << mgr->page_bits) < page_size )
	return mgr->err = BTERR_wrt;

  return 0;
}

//	decrement pin count

void bt_unpinlatch (BtLatchSet *latch)
{
	bt_mutexlock(latch->modify);
	latch->pin--;
	bt_releasemutex(latch->modify);
}

//  return the btree cached page address

BtPage bt_mappage (BtMgr *mgr, BtLatchSet *latch)
{
uint segment = latch->page_no >> SEG_bits;
int flag = PROT_READ | PROT_WRITE;
uid mask = (uid)1 << SEG_bits;
BtPage page;

  bt_mutexlock (mgr->maps);
  mask--;

  while( 1 ) {
	if( segment < mgr->segments ) {
	  page = (BtPage)(mgr->pages[segment] + ((latch->page_no & mask) << mgr->page_bits));

	  bt_releasemutex (mgr->maps);
	  return page;
	}

	if( mgr->segments < mgr->maxseg ) {
	  mgr->pages[mgr->segments] = mmap (0, (uid)mgr->page_size << SEG_bits, flag, MAP_SHARED, mgr->idx, (uid)mgr->segments << mgr->page_bits << SEG_bits);
	  mgr->segments++;
	  continue;
	}

	mgr->maxseg <<= 1;
	mgr->pages = realloc (mgr->pages, mgr->maxseg * sizeof(void *));
  }
}

//  return next available latch entry
//	  and with latch entry locked

uint bt_availnext (BtMgr *mgr)
{
BtLatchSet *latch;
uint entry;

  while( 1 ) {
#ifdef unix
	entry = __sync_fetch_and_add (&mgr->pagezero->latchvictim, 1) + 1;
#else
	entry = _InterlockedIncrement (&mgr->pagezero->latchvictim);
#endif
	entry %= mgr->pagezero->latchtotal;

	if( !entry )
		continue;

	latch = mgr->latchsets + entry;

	if( !bt_mutextry(latch->modify) )
		continue;

	//  return this entry if it is not pinned

	if( !latch->pin )
		return entry;

	bt_releasemutex(latch->modify);
  }
}

//	pin latch in latch pool

BtLatchSet *bt_pinlatch (BtMgr *mgr, uid page_no)
{
uint hashidx = page_no % mgr->pagezero->latchhash;
uint entry, oldidx;
BtLatchSet *latch;
BtPage page;

  //  try to find our entry

  bt_mutexlock(mgr->hashtable[hashidx].latch);

  if( entry = mgr->hashtable[hashidx].entry ) do
  {
	latch = mgr->latchsets + entry;

	if( page_no == latch->page_no )
		break;
  } while( entry = latch->next );

  //  found our entry: increment pin

  if( entry ) {
	latch = mgr->latchsets + entry;
	bt_mutexlock(latch->modify);
	latch->pin++;
	bt_releasemutex(latch->modify);
	bt_releasemutex(mgr->hashtable[hashidx].latch);
	return latch;
  }

  //  find and reuse unpinned entry

trynext:
  entry = bt_availnext (mgr);
  latch = mgr->latchsets + entry;
  oldidx = latch->page_no % mgr->pagezero->latchhash;

  //  skip over this entry if latch not available

  if( latch->page_no )
   if( oldidx != hashidx )
    if( !bt_mutextry (mgr->hashtable[oldidx].latch) ) {
	  bt_releasemutex(latch->modify);
	  goto trynext;
	}

  //  if latch is on a different hash chain
  //	unlink from the old page_no chain

  if( latch->page_no )
   if( oldidx != hashidx ) {
	if( latch->prev )
	  mgr->latchsets[latch->prev].next = latch->next;
	else
	  mgr->hashtable[oldidx].entry = latch->next;

	if( latch->next )
	  mgr->latchsets[latch->next].prev = latch->prev;

    bt_releasemutex (mgr->hashtable[oldidx].latch);
   }

  //  link page as head of hash table chain
  //  if this is a never before used entry,
  //  or it was previously on a different
  //  hash table chain. Otherwise, just
  //  leave it in its current hash table
  //  chain position.

  if( !latch->page_no || hashidx != oldidx ) {
	if( latch->next = mgr->hashtable[hashidx].entry )
	  mgr->latchsets[latch->next].prev = entry;

	mgr->hashtable[hashidx].entry = entry;
    latch->prev = 0;
  }

  //  fill in latch structure

  latch->page_no = page_no;
  latch->pin = 1;

  bt_releasemutex (latch->modify);
  bt_releasemutex (mgr->hashtable[hashidx].latch);
  return latch;
}
  
void bt_mgrclose (BtMgr *mgr)
{
char *name = mgr->type ? "Main" : "Cache";
BtLatchSet *latch;
uint num = 0;
BtPage page;
uint entry;

	//	flush previously written dirty pages
	//	and write recovery buffer to disk

	fdatasync (mgr->idx);

#ifdef unix
	while( mgr->segments )
		munmap (mgr->pages[--mgr->segments], (uid)mgr->page_size << SEG_bits);
#else
	while( mgr->segments ) {
		FlushViewOfFile(mgr->pages[--mgr->segments], 0);
		UnmapViewOfFile(mgr->pages[mgr->Segments]);
	}
#endif
#ifdef unix
	close (mgr->idx);
	free (mgr);
#else
	FlushFileBuffers(mgr->idx);
	CloseHandle(mgr->idx);
	GlobalFree (mgr);
#endif
}

//	close and release memory

void bt_close (BtDb *bt)
{
	free (bt);
}

void bt_initpage (BtMgr *mgr, BtPage page, uid leaf_page_no, uint lvl)
{
BtSlot *node = slotptr(page, 1);
unsigned char value[BtId];
uid page_no;
BtKey* key;
BtVal *val;

	page_no = lvl ? ROOT_page : leaf_page_no;
	node->off = mgr->page_size;

	if( !lvl )
		node->off <<= mgr->leaf_xtra;

	node->off -= 3 + (lvl ? BtId + sizeof(BtVal): sizeof(BtVal));
	node->type = Librarian;
	node++->dead = 1;

	node->off = node[-1].off;
	key = keyptr(page, 2);
	key = keyptr(page, 1);
	key->len = 2;		// create stopper key
	key->key[0] = 0xff;
	key->key[1] = 0xff;

	bt_putid(value, leaf_page_no);
	val = valptr(page, 1);
	val->len = lvl ? BtId : 0;
	memcpy (val->value, value, val->len);

	page->fence = node->off;
	page->min = node->off;
	page->lvl = lvl;
	page->cnt = 2;
	page->act = 1;

	if( bt_writepage (mgr, page, page_no, !lvl) ) {
		fprintf (stderr, "Unable to create btree page %d\n", page_no);
		exit(0);
	}
}

//  open/create new btree buffer manager

//	call with file_name, BT_openmode, bits in page size (e.g. 16),
//		extra bits for leaves (e.g. 4) size of latch pool (e.g. 500)

BtMgr *bt_mgr (char *name, uint pagebits, uint leafxtra, uint nodemax)
{
uint lvl, attr, last, slot, idx, blk;
int flag, initit = 0;
BtPageZero *pagezero;
struct flock lock[1];
BtLatchSet *latch;
uid leaf_page;
off64_t size;
BtPage page;
uint amt[1];
BtMgr* mgr;

	// determine sanity of page size and buffer pool

	if( leafxtra | pagebits )
	  if( leafxtra + pagebits > BT_maxbits )
		fprintf (stderr, "pagebits + leafxtra > maxbits\n"), exit(1);

	if( pagebits )
	  if( pagebits < BT_minbits )
		fprintf (stderr, "pagebits < minbits\n"), exit(1);

#ifdef unix
	mgr = calloc (1, sizeof(BtMgr));

	mgr->idx = open ((char*)name, O_RDWR | O_CREAT, 0666);

	if( mgr->idx == -1 ) {
		fprintf (stderr, "Unable to create/open btree file %s\n", name);
		return free(mgr), NULL;
	}

	memset (lock, 0, sizeof(lock));
	lock->l_len = sizeof(struct BtPage_);
	lock->l_type = F_WRLCK;

	if( fcntl (mgr->idx, F_SETLKW, lock) < 0 ) {
		fprintf(stderr, "unable to lock record zero %s\n", name);
		exit(1);
	}
#else
	mgr = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BtMgr));
	attr = FILE_ATTRIBUTE_NORMAL;
	mgr->idx = CreateFile(name, GENERIC_READ| GENERIC_WRITE, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, attr, NULL);

	if( mgr->idx == INVALID_HANDLE_VALUE ) {
		fprintf (stderr, "Unable to create/open btree file %s\n", name);
		return GlobalFree(mgr), NULL;
	}
#endif

#ifdef unix
	pagezero = valloc (BT_maxpage);
	page = (BtPage)pagezero;
	*amt = 0;

	// read minimum page size to get root info
	//	to support raw disk partition files
	//	check if page_bits == 0 on the disk.

	if( size = lseek (mgr->idx, 0L, 2) )
		if( pread(mgr->idx, pagezero, BT_minpage, 0) == BT_minpage )
			if( pagezero->page_bits ) {
				pagebits = pagezero->page_bits;
				leafxtra = pagezero->leaf_xtra;
			} else
				initit = 1;
		else
			return free(mgr), free(pagezero), NULL;
	else
		initit = 1;
#else
	pagezero = VirtualAlloc(NULL, BT_maxpage, MEM_COMMIT, PAGE_READWRITE);
	size = GetFileSize(mgr->idx, amt);

	if( size || *amt ) {
		if( !ReadFile(mgr->idx, (char *)pagezero, BT_minpage, amt, NULL) )
			return bt_mgrclose (mgr), NULL;
		pagebits = pagezero->page_bits;
		leafxtra = pagezero->leaf_xtra;
	} else
		initit = 1;
#endif

	mgr->page_size = 1 << pagebits;
	mgr->page_bits = pagebits;
	mgr->leaf_xtra = leafxtra;

	if( !initit )
		goto mgrlatch;

	//  calculate number of latch table & hash entries

	memset (pagezero, 0, 1 << pagebits);
	pagezero->nlatchpage = nodemax/16 * sizeof(BtHashEntry);

	pagezero->nlatchpage += sizeof(BtLatchSet) * nodemax + mgr->page_size - 1;
	pagezero->nlatchpage >>= mgr->page_bits;
	pagezero->latchtotal = nodemax;

	pagezero->latchhash = (((uid)pagezero->nlatchpage<< mgr->page_bits) - nodemax * sizeof(BtLatchSet)) / sizeof(BtHashEntry);

	// initialize an empty b-tree with alloc page, root page, leaf page
	// and page(s) of latches and page pool cache

	pagezero->page_bits = mgr->page_bits;
	pagezero->leaf_xtra = leafxtra;
	pagezero->upperpages = 1;
	pagezero->leafpages = 1;

	leaf_page = pagezero->leaf_page = pagezero->nlatchpage + LATCH_page;

	//	round first leafpage up to leafxtra boundary

	if( pagezero->leaf_page & ((1 << leafxtra) - 1)) {
		blk = pagezero->leaf_page;
		pagezero->leaf_page |= (1 << leafxtra) - 1;
		pagezero->freechain = pagezero->leaf_page++;
		leaf_page = pagezero->leaf_page;
	} else
		blk = 0;

	pagezero->rightleaf = pagezero->leaf_page;
	pagezero->allocpage = pagezero->leaf_page + (1 << leafxtra);

	if( pwrite (mgr->idx, pagezero, 1 << pagebits, 0) < 1 << pagebits) {
		fprintf (stderr, "Unable to create btree page zero\n");
		return bt_mgrclose (mgr), NULL;
	}

	//	initialize root level 1 page

	memset (page, 0, 1 << pagebits);
	bt_initpage (mgr, page, leaf_page, 1);

	//  chain unused pages as first freelist

	memset (page, 0, 1 << pagebits);

	while( blk & ((1 << leafxtra) - 1) ) {
	  if( bt_writepage (mgr, page, blk, 0) ) {
		fprintf(stderr, "unable to write initial free blk %d\r\n", blk);
		exit(1);
	  }
	  page->right = blk++;
	}

	// initialize first page of leaves

	memset (page, 0, 1 << pagebits);
	bt_initpage (mgr, page, leaf_page, 0);

mgrlatch:
#ifdef unix
	free (pagezero);
#else
	VirtualFree (pagezero, 0, MEM_RELEASE);
#endif

	lock->l_type = F_UNLCK;

	if( fcntl (mgr->idx, F_SETLK, lock) < 0 ) {
		fprintf (stderr, "Unable to unlock page zero\n");
		exit(1);
	}

	//	map first segment

	mgr->segments = 1;
	mgr->maxseg = MIN_seg;
	mgr->pages = calloc (MIN_seg, sizeof(unsigned char *));

	flag = PROT_READ | PROT_WRITE;
	mgr->pages[0] = mmap (0, (uid)mgr->page_size << SEG_bits, flag, MAP_SHARED, mgr->idx, 0);

	if( mgr->pages[0] == MAP_FAILED ) {
		fprintf (stderr, "Unable to mmap pagezero btree segment, error = %d\n", errno);
		return bt_mgrclose (mgr), NULL;
	}

	mgr->pagezero = (BtPageZero *)mgr->pages[0];
//	mlock (mgr->pagezero, mgr->page_size);

	//	allocate latch pool

	mgr->latchsets = (BtLatchSet *)(mgr->pages[0] + ((uid)LATCH_page << mgr->page_bits));
	mgr->hashtable = (BtHashEntry *)(mgr->latchsets + mgr->pagezero->latchtotal);

	return mgr;
}

//	open BTree access method
//	based on buffer manager

BtDb *bt_open (BtMgr *mgr, BtMgr *main)
{
BtDb *bt = malloc (sizeof(*bt));

	memset (bt, 0, sizeof(*bt));
	bt->tid = sys_gettid();
	bt->main = main;
	bt->mgr = mgr;
	return bt;
}

//  compare two keys, return > 0, = 0, or < 0
//  =0: keys are same
//  -1: key2 > key1
//  +1: key2 < key1
//  as the comparison value

int keycmp (BtKey* key1, unsigned char *key2, uint len2)
{
uint len1 = key1->len;
int ans;

	if( ans = memcmp (key1->key, key2, len1 > len2 ? len2 : len1) )
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

// place write, read, or parent lock on requested page_no.

void bt_lockpage(BtLock mode, BtLatchSet *latch, pid_t tid, uint line)
{
	switch( mode ) {
	case BtLockRead:
		ReadLock (latch->readwr);
		break;
	case BtLockWrite:
		WriteLock (latch->readwr, tid, line);
		break;
	case BtLockAccess:
		ReadLock (latch->access);
		break;
	case BtLockDelete:
		WriteLock (latch->access, tid, line);
		break;
	case BtLockParent:
		WriteLock (latch->parent, tid, line);
		break;
	case BtLockLink:
		WriteLock (latch->link, tid, line);
		break;
	}
}

// remove write, read, or parent lock on requested page

void bt_unlockpage(BtLock mode, BtLatchSet *latch, uint line)
{
	switch( mode ) {
	case BtLockRead:
		ReadRelease (latch->readwr);
		break;
	case BtLockWrite:
		WriteRelease (latch->readwr);
		break;
	case BtLockAccess:
		ReadRelease (latch->access);
		break;
	case BtLockDelete:
		WriteRelease (latch->access);
		break;
	case BtLockParent:
		WriteRelease (latch->parent);
		break;
	case BtLockLink:
		WriteRelease (latch->link);
		break;
	}
}

//	allocate a new page
//	return with page latched, but unlocked.
//	contents is cleared for lvl > 0

int bt_newpage(BtMgr *mgr, BtPageSet *set, BtPage contents)
{
uint page_size = mgr->page_size, blk;
uid *freechain;
uid page_no;

	//	lock allocation page

	bt_mutexlock(mgr->pagezero->lock);

	if( contents->lvl ) {
		freechain = &mgr->pagezero->freechain;
		mgr->pagezero->upperpages++;
	} else {
		freechain = &mgr->pagezero->leafchain;
		mgr->pagezero->leafpages++;
		page_size <<= mgr->leaf_xtra;
	}

	// use empty chain first
	// else allocate new page

	if( page_no = *freechain ) {
		if( set->latch = bt_pinlatch (mgr, page_no) )
			set->page = bt_mappage (mgr, set->latch);
		else
			return mgr->line = __LINE__, mgr->err = BTERR_struct;

		*freechain = set->page->right;

		//  the page is currently nopromote and this
		//  will keep bt_promote out.

		//	contents will replace this bit
		//  and pin will keep bt_promote out

		contents->nopromote = 0;

		memcpy (set->page, contents, page_size);

//		if( msync (mgr->pagezero, mgr->page_size, MS_SYNC) < 0 )
//		  fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);

		bt_releasemutex(mgr->pagezero->lock);
		return 0;
	}

	//  obtain next available page number
	//	suitable for leaf or higher level

	page_no = mgr->pagezero->allocpage;
	mgr->pagezero->allocpage += 1 << mgr->leaf_xtra;

	//	keep bt_promote out of this page

	contents->nopromote = 1;

	// unlock allocation latch and
	//	extend file into new page.

//	if( msync (mgr->pagezero, mgr->page_size, MS_SYNC) < 0 )
//	  fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);

	if( bt_writepage (mgr, contents, page_no, !contents->lvl) )
		fprintf(stderr, "Write %lld error %d\n", page_no + blk, errno);

	//	chain together unused non-leaf allocation

	if( contents->lvl ) {
	  memset (contents, 0, mgr->page_size);

	  for( blk = 1; blk < 1 << mgr->leaf_xtra; blk++ ) {
		if( bt_writepage (mgr, contents, page_no + blk, 0) )
		  fprintf(stderr, "Write %lld error %d\n", page_no + blk, errno);
		contents->right = page_no + blk;
		*freechain = page_no + blk;
	  }
	}

	bt_releasemutex(mgr->pagezero->lock);

	if( set->latch = bt_pinlatch (mgr, page_no) )
		set->page = bt_mappage (mgr, set->latch);
	else
		return mgr->err;

	// now pin will keep bt_promote out

	set->page->nopromote = 0;
	return 0;
}

//  find slot in page for given key at a given level

int bt_findslot (BtPage page, unsigned char *key, uint len)
{
uint diff, higher = page->cnt, low = 1, slot;
uint good = 0;

	//	  make stopper key an infinite fence value

	if( page->right )
		higher++;
	else
		good++;

	//	low is the lowest candidate.
	//  loop ends when they meet

	//  higher is already
	//	tested as .ge. the passed key.

	while( diff = higher - low ) {
		slot = low + ( diff >> 1 );
		if( keycmp (keyptr(page, slot), key, len) < 0 )
			low = slot + 1;
		else
			higher = slot, good++;
	}

	//	return zero if key is on right link page

	return good ? higher : 0;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

int bt_loadpage (BtMgr *mgr, BtPageSet *set, unsigned char *key, uint len, uint lvl, BtLock lock, pid_t tid)
{
uid page_no = ROOT_page, prevpage_no = 0;
uint drill = 0xff, slot;
uint mode, prevmode;
BtPageSet prev[1];
BtVal *val;
BtKey *ptr;

  //  start at root of btree and drill down

  do {
	if( set->latch = bt_pinlatch (mgr, page_no) )
	  set->page = bt_mappage (mgr, set->latch);
	else
	  return 0;

	if( page_no > ROOT_page )
	  bt_lockpage(BtLockAccess, set->latch, tid, __LINE__);

	//	release & unpin parent or left sibling page

	if( prevpage_no ) {
	  bt_unlockpage(prevmode, prev->latch, __LINE__);
	  bt_unpinlatch (prev->latch);
	  prevpage_no = 0;
	}

 	// obtain mode lock using lock coupling through AccessLock
	// determine lock mode of drill level

	mode = (drill == lvl) ? lock : BtLockRead; 
	bt_lockpage(mode, set->latch, tid, __LINE__);

	// grab our fence key

	ptr=fenceptr(set->page);

	if( set->page->free )
		return mgr->err = BTERR_struct, mgr->line = __LINE__, 0;

	if( page_no > ROOT_page )
	  bt_unlockpage(BtLockAccess, set->latch, __LINE__);

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		if( set->latch->page_no != ROOT_page )
			return mgr->err = BTERR_struct, mgr->line = __LINE__, 0;
			
		drill = set->page->lvl;

		if( lock != BtLockRead && drill == lvl ) {
		  bt_unlockpage(mode, set->latch, __LINE__);
		  bt_unpinlatch (set->latch);
		  continue;
		}
	}

	prevpage_no = set->latch->page_no;
	prevmode = mode;
	*prev = *set;

	//  if requested key is beyond our fence,
	//	slide to the right

	if( keycmp (ptr, key, len) < 0 )
	  if( page_no = set->page->right )
	  	continue;

	//  if page is part of a delete operation,
	//	slide to the left;

	if( set->page->kill ) {
	  bt_lockpage(BtLockLink, set->latch, tid, __LINE__);
	  page_no = set->page->left;
	  bt_unlockpage(BtLockLink, set->latch, __LINE__);
	  continue;
	}

	//  find key on page at this level
	//  and descend to requested level

	if( slot = bt_findslot (set->page, key, len) ) {
	  if( drill == lvl )
		return slot;

	  // find next non-dead slot -- the fence key if nothing else

	  while( slotptr(set->page, slot)->dead )
		if( slot++ < set->page->cnt )
		  continue;
		else
  		  return mgr->err = BTERR_struct, mgr->line = __LINE__, 0;

	  val = valptr(set->page, slot);

	  if( val->len == BtId )
	  	page_no = bt_getid(val->value);
	  else
  		return mgr->line = __LINE__, mgr->err = BTERR_struct, 0;

	  drill--;
	  continue;
	 }

	//  slide right into next page

	page_no = set->page->right;

  } while( page_no );

  // return error on end of right chain

  mgr->line = __LINE__, mgr->err = BTERR_struct;
  return 0;	// return error
}

//	return page to free list
//	page must be delete, link & write locked
//	and have no keys pointing to it.

void bt_freepage (BtMgr *mgr, BtPageSet *set)
{
uid *freechain;

	//	lock allocation page

	bt_mutexlock (mgr->pagezero->lock);

	if( set->page->lvl ) {
		freechain = &mgr->pagezero->freechain;
		mgr->pagezero->upperpages--;
	} else {
		freechain = &mgr->pagezero->leafchain;
		mgr->pagezero->leafpages--;
	}

	//	store chain link

	set->page->right = *freechain;
	*freechain = set->latch->page_no;
	set->page->free = 1;

//	if( msync (mgr->pagezero, mgr->page_size, MS_SYNC) < 0 )
//	  fprintf(stderr, "msync error %d line %d\n", errno, __LINE__);

	// unlock released page
	// and unlock allocation page

	bt_unlockpage (BtLockDelete, set->latch, __LINE__);
	bt_unlockpage (BtLockWrite, set->latch, __LINE__);
	bt_unlockpage (BtLockLink, set->latch, __LINE__);
	bt_unpinlatch (set->latch);
	bt_releasemutex (mgr->pagezero->lock);
}

//	a fence key was deleted from an interiour level page
//	push new fence value upwards

BTERR bt_fixfence (BtMgr *mgr, BtPageSet *set, uint lvl)
{
unsigned char leftkey[BT_keyarray], rightkey[BT_keyarray];
unsigned char value[BtId];
BtKey* ptr;
uint idx;

	//	remove the old fence value

	ptr = fenceptr(set->page);
	memcpy (rightkey, ptr, ptr->len + sizeof(BtKey));
	memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
	set->page->fence = slotptr(set->page, set->page->cnt)->off;

	//  cache new fence value

	ptr = fenceptr(set->page);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	bt_lockpage (BtLockParent, set->latch, 0, __LINE__);
	bt_unlockpage (BtLockWrite, set->latch, __LINE__);

	//	insert new (now smaller) fence key

	bt_putid (value, set->latch->page_no);
	ptr = (BtKey*)leftkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique) )
	  return mgr->err;

	//	now delete old fence key

	ptr = (BtKey*)rightkey;

	if( bt_deletekey (mgr, ptr->key, ptr->len, lvl+1) )
		return mgr->err;

	bt_unlockpage (BtLockParent, set->latch, __LINE__);
	bt_unpinlatch(set->latch);
	return 0;
}

//	root has a single child
//	collapse a level from the tree

BTERR bt_collapseroot (BtMgr *mgr, BtPageSet *root)
{
BtPageSet child[1];
uid page_no;
BtVal *val;
uint idx;

  // find the child entry and promote as new root contents

  do {
	for( idx = 0; idx++ < root->page->cnt; )
	  if( !slotptr(root->page, idx)->dead )
		break;

	val = valptr(root->page, idx);

	if( val->len == BtId )
		page_no = bt_getid (valptr(root->page, idx)->value);
	else
  		return mgr->line = __LINE__, mgr->err = BTERR_struct;

	if( child->latch = bt_pinlatch (mgr, page_no) )
		child->page = bt_mappage (mgr, child->latch);
	else
		return mgr->err;

	bt_lockpage (BtLockDelete, child->latch, 0, __LINE__);
	bt_lockpage (BtLockWrite, child->latch, 0, __LINE__);

	memcpy (root->page, child->page, mgr->page_size);
	bt_freepage (mgr, child);

  } while( root->page->lvl > 1 && root->page->act == 1 );

  bt_unlockpage (BtLockWrite, root->latch, __LINE__);
  bt_unpinlatch (root->latch);
  return 0;
}

//  delete a page and manage key
//  call with page writelocked

//	returns with page unpinned
//	from the page pool.

BTERR bt_deletepage (BtMgr *mgr, BtPageSet *set, uint lvl)
{
unsigned char higherfence[BT_keyarray], lowerfence[BT_keyarray];
uint page_size = mgr->page_size, kill;
BtPageSet right[1], temp[1];
unsigned char value[BtId];
uid page_no, right2;
BtKey *ptr;

	if( !lvl )
		page_size <<= mgr->leaf_xtra;

	//  cache original copy of original fence key
	//	that is going to be deleted.

	ptr = fenceptr(set->page);
	memcpy (lowerfence, ptr, ptr->len + sizeof(BtKey));

	//	pin and lock our right page

	page_no = set->page->right;

	if( right->latch = bt_pinlatch (mgr, page_no) )
		right->page = bt_mappage (mgr, right->latch);
	else
		return 0;

	bt_lockpage (BtLockWrite, right->latch, 0, __LINE__);

	if( right->page->kill || set->page->kill )
		return mgr->line = __LINE__, mgr->err = BTERR_struct;

	// pull contents of right sibling over our empty page
	//	preserving our left page number, and its right page number.

	bt_lockpage (BtLockLink, set->latch, 0, __LINE__);
	page_no = set->page->left;
	memcpy (set->page, right->page, page_size);
	set->page->left = page_no;
	bt_unlockpage (BtLockLink, set->latch, __LINE__);

	//  fix left link from far right page

	if( right2 = set->page->right ) {
	  if( temp->latch = bt_pinlatch (mgr, right2) )
		temp->page = bt_mappage (mgr, temp->latch);
	  else
		return 0;

	  bt_lockpage (BtLockAccess, temp->latch, 0, __LINE__);
      bt_lockpage(BtLockLink, temp->latch, 0, __LINE__);
	  temp->page->left = set->latch->page_no;
	  bt_unlockpage(BtLockLink, temp->latch, __LINE__);
	  bt_unlockpage(BtLockAccess, temp->latch, __LINE__);
	  bt_unpinlatch (temp->latch);
	} else if( !lvl ) {	// our page is now rightmost leaf
	  bt_mutexlock (mgr->pagezero->lock);
	  mgr->pagezero->rightleaf = set->latch->page_no;
	  bt_releasemutex(mgr->pagezero->lock);
	}

	ptr = fenceptr(set->page);
	memcpy (higherfence, ptr, ptr->len + sizeof(BtKey));

	// mark right page as being deleted and release lock
	//	keep lock on parent modification.

	right->page->kill = 1;
	bt_lockpage (BtLockParent, right->latch, 0, __LINE__);
	bt_unlockpage (BtLockWrite, right->latch, __LINE__);

	bt_lockpage (BtLockParent, set->latch, 0, __LINE__);
	bt_unlockpage (BtLockWrite, set->latch, __LINE__);

	// redirect the new higher key directly to our new node

	ptr = (BtKey *)higherfence;
	bt_putid (value, set->latch->page_no);

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Update) )
	  return mgr->err;

	//	delete our original fence key in parent

	ptr = (BtKey *)lowerfence;

	if( bt_deletekey (mgr, ptr->key, ptr->len, lvl+1) )
	  return mgr->err;

	//  wait for all access to drain away with delete lock,
	//	then obtain write lock to right node and free it.

	bt_lockpage (BtLockDelete, right->latch, 0, __LINE__);
	bt_lockpage (BtLockWrite, right->latch, 0, __LINE__);
	bt_lockpage (BtLockLink, right->latch, 0, __LINE__);
	bt_unlockpage (BtLockParent, right->latch, __LINE__);
	bt_freepage (mgr, right);

	//	release parent lock to our node

	bt_unlockpage (BtLockParent, set->latch, __LINE__);
	bt_unpinlatch (set->latch);
	return 0;
}

//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree

BTERR bt_deletekey (BtMgr *mgr, unsigned char *key, uint len, uint lvl)
{
uint slot, idx, found, fence, ptrlen;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;

	if( slot = bt_loadpage (mgr, set, key, len, lvl, BtLockWrite, 0) ) {
		node = slotptr(set->page, slot);
		ptr = keyptr(set->page, slot);
	} else
		return mgr->err;

	// if librarian slot, advance to real slot

	if( node->type == Librarian ) {
		ptr = keyptr(set->page, ++slot);
		node = slotptr(set->page, slot);
	}

	fence = slot == set->page->cnt;
	ptrlen = ptr->len;

	if( node->type == Duplicate )
		ptrlen -= BtId;

	// delete the key, ignore request if already dead

	if( found = !memcmp (ptr->key, key, ptrlen > len ? len : ptrlen) )
	  if( found = node->dead == 0 ) {
		val = valptr(set->page,slot);
 		set->page->garbage += ptr->len + val->len + sizeof(BtKey) + sizeof(BtVal);
 		set->page->act--;
		node->dead = 1;

		// collapse empty slots beneath the fence
		// on interiour nodes

		if( lvl )
		 while( idx = set->page->cnt - 1 )
		  if( slotptr(set->page, idx)->dead ) {
			*slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
			memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
		  } else
			break;
	  }

	if( !found )
		return 0;

	//	did we delete a fence key in an upper level?

	if( lvl && set->page->act && fence )
	  return bt_fixfence (mgr, set, lvl);

	//	do we need to collapse root?

	if( lvl > 1 && set->latch->page_no == ROOT_page && set->page->act == 1 )
	  return bt_collapseroot (mgr, set);

	//	delete empty page

 	if( !set->page->act )
	  return bt_deletepage (mgr, set, set->page->lvl);

	bt_unlockpage(BtLockWrite, set->latch, __LINE__);
	bt_unpinlatch (set->latch);
	return 0;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	>0  new slot value

uint bt_cleanpage(BtMgr *mgr, BtPageSet *set, uint keylen, uint slot, uint vallen)
{
uint page_size = mgr->page_size;
BtPage page = set->page, frame;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uint newslot = max;
BtKey *key;
BtVal *val;

	if( !set->page->lvl )
		page_size <<= mgr->leaf_xtra;

	if( page->min >= (max+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal))
		return slot;

	//	skip cleanup and proceed to split
	//	if there's not enough garbage
	//	to bother with.

	if( page->garbage < page_size / 5 )
		return 0;

	frame = malloc (page_size);
	memcpy (frame, page, page_size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, page_size - sizeof(*page));

	page->min = page_size;
	page->garbage = 0;
	page->act = 0;

	// clean up page first by
	// removing dead keys

	while( cnt++ < max ) {
		if( cnt == slot )
			newslot = idx + 2;

		if( cnt < max || frame->lvl )
		  if( slotptr(frame,cnt)->dead )
			continue;

		// copy the value across

		val = valptr(frame, cnt);
		page->min -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)page + page->min, val, val->len + sizeof(BtVal));

		// copy the key across

		key = keyptr(frame, cnt);
		page->min -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)page + page->min, key, key->len + sizeof(BtKey));

		// make a librarian slot

		slotptr(page, ++idx)->off = page->min;
		slotptr(page, idx)->type = Librarian;
		slotptr(page, idx)->dead = 1;

		// set up the slot

		slotptr(page, ++idx)->off = page->min;
		slotptr(page, idx)->type = slotptr(frame, cnt)->type;

		if( !(slotptr(page, idx)->dead = slotptr(frame, cnt)->dead) )
		  page->act++;
	}

	page->fence = page->min;
	page->cnt = idx;
	free (frame);

	//	see if page has enough space now, or does it need splitting?

	if( page->min >= (idx+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal) )
		return newslot;

	return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtMgr *mgr, BtPageSet *root, BtLatchSet *right)
{  
unsigned char leftkey[BT_keyarray];
uint nxt = mgr->page_size;
unsigned char value[BtId];
BtPage frame, page;
BtPageSet left[1];
uid left_page_no;
BtKey *ptr;
BtVal *val;

	frame = malloc (mgr->page_size);
	memcpy (frame, root->page, mgr->page_size);

	//	save left page fence key for new root

	ptr = fenceptr(root->page);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	if( bt_newpage(mgr, left, frame) )
		return mgr->err;

	left_page_no = left->latch->page_no;
	bt_unpinlatch (left->latch);
	free (frame);

	//	left link the pages together

	page = bt_mappage (mgr, right);
	page->left = left_page_no;

	// preserve the page info at the bottom
	// of higher keys and set rest to zero

	memset(root->page+1, 0, mgr->page_size - sizeof(*root->page));

	// insert stopper key at top of newroot page
	// and increase the root height

	nxt -= BtId + sizeof(BtVal);
	bt_putid (value, right->page_no);
	val = (BtVal *)((unsigned char *)root->page + nxt);
	memcpy (val->value, value, BtId);
	val->len = BtId;

	nxt -= 2 + sizeof(BtKey);
	root->page->fence = nxt;

	slotptr(root->page, 2)->off = nxt;
	ptr = (BtKey *)((unsigned char *)root->page + nxt);
	ptr->len = 2;
	ptr->key[0] = 0xff;
	ptr->key[1] = 0xff;

	// insert lower keys page fence key on newroot page as first key

	nxt -= BtId + sizeof(BtVal);
	bt_putid (value, left_page_no);
	val = (BtVal *)((unsigned char *)root->page + nxt);
	memcpy (val->value, value, BtId);
	val->len = BtId;

	ptr = (BtKey *)leftkey;
	nxt -= ptr->len + sizeof(BtKey);
	slotptr(root->page, 1)->off = nxt;
	memcpy ((unsigned char *)root->page + nxt, leftkey, ptr->len + sizeof(BtKey));
	
	root->page->right = 0;
	root->page->min = nxt;		// reset lowest used offset and key count
	root->page->cnt = 2;
	root->page->act = 2;
	root->page->lvl++;

	// release and unpin root pages

	bt_unlockpage(BtLockWrite, root->latch, __LINE__);
	bt_unpinlatch (root->latch);

	bt_unpinlatch (right);
	return 0;
}

//  split already locked full node
//	leave it locked.
//	return pool entry for new right
//	page, pinned & unlocked

uint bt_splitpage (BtMgr *mgr, BtPageSet *set, uint linkleft)
{
uint page_size = mgr->page_size;
BtPageSet right[1], temp[1];
uint cnt = 0, idx = 0, max;
uint lvl = set->page->lvl;
BtPage frame;
BtKey *key;
BtVal *val;
uid right2;
uint entry;
uint prev;

	if( !set->page->lvl )
		page_size <<= mgr->leaf_xtra;

	//  split higher half of keys to frame

	frame = malloc (page_size);
	memset (frame, 0, page_size);
	frame->min = page_size;
	max = set->page->cnt;
	cnt = max / 2;
	idx = 0;

	while( cnt++ < max ) {
		if( cnt < max || set->page->lvl )
		  if( slotptr(set->page, cnt)->dead )
			continue;

		val = valptr(set->page, cnt);
		frame->min -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)frame + frame->min, val, val->len + sizeof(BtVal));

		key = keyptr(set->page, cnt);
		frame->min -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)frame + frame->min, key, key->len + sizeof(BtKey));

		//	add librarian slot

		slotptr(frame, ++idx)->off = frame->min;
		slotptr(frame, idx)->type = Librarian;
		slotptr(frame, idx)->dead = 1;

		//  add actual slot

		slotptr(frame, ++idx)->off = frame->min;
		slotptr(frame, idx)->type = slotptr(set->page, cnt)->type;

		if( !(slotptr(frame, idx)->dead = slotptr(set->page, cnt)->dead) )
		  frame->act++;
	}

	frame->fence = frame->min;
	frame->cnt = idx;
	frame->lvl = lvl;

	// link right node

	if( set->latch->page_no > ROOT_page ) {
		right2 = set->page->right;
		frame->right = right2;

		if( linkleft )
			frame->left = set->latch->page_no;
	}

	//	get new free page and write higher keys to it.

	if( bt_newpage(mgr, right, frame) )
		return 0;

	//	link far right's left pointer to new page

	if( linkleft && set->latch->page_no > ROOT_page )
	 if( right2 ) {
	  if( temp->latch = bt_pinlatch (mgr, right2) )
		temp->page = bt_mappage (mgr, temp->latch);
	  else
		return 0;

      bt_lockpage(BtLockLink, temp->latch, 0, __LINE__);
	  temp->page->left = right->latch->page_no;
	  bt_unlockpage(BtLockLink, temp->latch, __LINE__);
	  bt_unpinlatch (temp->latch);
	 } else if( !lvl ) {	// page is rightmost leaf
	  bt_mutexlock (mgr->pagezero->lock);
	  mgr->pagezero->rightleaf = right->latch->page_no;
	  bt_releasemutex(mgr->pagezero->lock);
	 }

	// process lower keys

	memcpy (frame, set->page, page_size);
	memset (set->page+1, 0, page_size - sizeof(*set->page));

	set->page->min = page_size;
	set->page->garbage = 0;
	set->page->act = 0;
	max /= 2;
	cnt = 0;
	idx = 0;

	//  assemble page of smaller keys

	while( cnt++ < max ) {
		if( slotptr(frame, cnt)->dead )
			continue;
		val = valptr(frame, cnt);
		set->page->min -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)set->page + set->page->min, val, val->len + sizeof(BtVal));

		key = keyptr(frame, cnt);
		set->page->min -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)set->page + set->page->min, key, key->len + sizeof(BtKey));

		//	add librarian slot

		slotptr(set->page, ++idx)->off = set->page->min;
		slotptr(set->page, idx)->type = Librarian;
		slotptr(set->page, idx)->dead = 1;

		//	add actual slot

		slotptr(set->page, ++idx)->off = set->page->min;
		slotptr(set->page, idx)->type = slotptr(frame, cnt)->type;
		set->page->act++;
	}

	set->page->right = right->latch->page_no;
	set->page->fence = set->page->min;
	set->page->cnt = idx;
	free(frame);

	entry = right->latch - mgr->latchsets;
	return entry;
}

//	fix keys for newly split page
//	call with both pages pinned & locked
//  return unlocked and unpinned

BTERR bt_splitkeys (BtMgr *mgr, BtPageSet *set, BtLatchSet *right)
{
unsigned char leftkey[BT_keyarray], rightkey[BT_keyarray];
unsigned char value[BtId];
uint lvl = set->page->lvl;
BtPageSet temp[1];
BtPage page;
BtKey *ptr;
uid right2;

	// if current page is the root page, split it

	if( set->latch->page_no == ROOT_page )
		return bt_splitroot (mgr, set, right);

	ptr = fenceptr(set->page);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	page = bt_mappage (mgr, right);

	ptr = fenceptr(page);
	memcpy (rightkey, ptr, ptr->len + sizeof(BtKey));

	//	splice in far right page's left page_no

	if( right2 = page->right ) {
	  if( temp->latch = bt_pinlatch (mgr, right2) )
		temp->page = bt_mappage (mgr, temp->latch);
	  else
		return 0;

      bt_lockpage(BtLockLink, temp->latch, 0, __LINE__);
	  temp->page->left = right->page_no;
	  bt_unlockpage(BtLockLink, temp->latch, __LINE__);
	  bt_unpinlatch (temp->latch);
	} else if( !lvl ) {  // right page is far right page
	  bt_mutexlock (mgr->pagezero->lock);
	  mgr->pagezero->rightleaf = right->page_no;
	  bt_releasemutex(mgr->pagezero->lock);
	}
	// insert new fences in their parent pages

	bt_lockpage (BtLockParent, right, 0, __LINE__);

	bt_lockpage (BtLockParent, set->latch, 0, __LINE__);
	bt_unlockpage (BtLockWrite, set->latch, __LINE__);

	// insert new fence for reformulated left block of smaller keys

	bt_putid (value, set->latch->page_no);
	ptr = (BtKey *)leftkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique) )
		return mgr->err;

	// switch fence for right block of larger keys to new right page

	bt_putid (value, right->page_no);
	ptr = (BtKey *)rightkey;

	if( bt_insertkey (mgr, ptr->key, ptr->len, lvl+1, value, BtId, Unique) )
		return mgr->err;

	bt_unlockpage (BtLockParent, set->latch, __LINE__);
	bt_unpinlatch (set->latch);

	bt_unlockpage (BtLockParent, right, __LINE__);
	bt_unpinlatch (right);
	return 0;
}

//	install new key and value onto page
//	page must already be checked for
//	adequate space

BTERR bt_insertslot (BtMgr *mgr, BtPageSet *set, uint slot, unsigned char *key,uint keylen, unsigned char *value, uint vallen, uint type)
{
uint idx, librarian;
BtSlot *node;
BtKey *ptr;
BtVal *val;
int rate;

	//	if previous slot is a librarian slot, use it

	if( slot > 1 )
	  if( slotptr(set->page, slot-1)->type == Librarian )
		slot--;

	// copy value onto page

	set->page->min -= vallen + sizeof(BtVal);
	val = (BtVal*)((unsigned char *)set->page + set->page->min);
	memcpy (val->value, value, vallen);
	val->len = vallen;

	// copy key onto page

	set->page->min -= keylen + sizeof(BtKey);
	ptr = (BtKey*)((unsigned char *)set->page + set->page->min);
	memcpy (ptr->key, key, keylen);
	ptr->len = keylen;
	
	//	find first empty slot at or above our insert slot

	for( idx = slot; idx < set->page->cnt; idx++ )
	  if( slotptr(set->page, idx)->dead )
		break;

	// now insert key into array before slot.

	//	if we're going all the way to the top,
	//	add as many librarian slots as
	//	makes sense.

	if( idx == set->page->cnt ) {
	int avail = 4 * set->page->min / 5 - sizeof(*set->page) - ++set->page->cnt * sizeof(BtSlot);

		librarian = ++idx - slot;
		avail /= sizeof(BtSlot);

		if( avail < 0 )
			avail = 0;

		if( librarian > avail )
			librarian = avail;

		if( librarian ) {
			rate = (idx - slot) / librarian;
			set->page->cnt += librarian;
			idx += librarian;
		} else
			rate = 0;
	} else
		librarian = 0, rate = 0;

	//  transfer slots and add librarian slots

	while( idx > slot ) {
		*slotptr(set->page, idx) = *slotptr(set->page, idx-librarian-1);

		//	add librarian slot per rate

		if( librarian )
		 if( (idx - slot)/2 <= librarian * rate ) {
			node = slotptr(set->page, --idx);
			node->off = node[1].off;
			node->type = Librarian;
			node->dead = 1;
			librarian--;
		  }

		--idx;
	}

	set->page->act++;

	//	fill in new slot

	node = slotptr(set->page, slot);
	node->off = set->page->min;
	node->type = type;
	node->dead = 0;
	return 0;
}

//  Insert new key into the btree at given level.
//	either add a new key or update/add an existing one

BTERR bt_insertkey (BtMgr *mgr, unsigned char *key, uint keylen, uint lvl, void *value, uint vallen, BtSlotType type)
{
uint slot, idx, len, entry;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;

  while( 1 ) { // find the page and slot for the current key
	if( slot = bt_loadpage (mgr, set, key, keylen, lvl, BtLockWrite, 0) ) {
		node = slotptr(set->page, slot);
		ptr = keyptr(set->page, slot);
	} else
		return mgr->err;

	// if librarian slot == found slot, advance to real slot

	if( node->type == Librarian ) {
		node = slotptr(set->page, ++slot);
		ptr = keyptr(set->page, slot);
	}

	//  if inserting a duplicate key or unique
	//	key that doesn't exist on the page,
	//	check for adequate space on the page
	//	and insert the new key before slot.

	switch( type ) {
	case Unique:
	case Duplicate:
	  if( !keycmp (ptr, key, keylen) )
		break;

	  if( slot = bt_cleanpage (mgr, set, keylen, slot, vallen) )
	    if( bt_insertslot (mgr, set, slot, key, keylen, value, vallen, type) )
		  return mgr->err;
		else
		  goto insxit;

	  if( entry = bt_splitpage (mgr, set, 1) )
	    if( !bt_splitkeys (mgr, set, entry + mgr->latchsets) )
		  continue;

	  return mgr->err;

	case Update:
	  if( keycmp (ptr, key, keylen) )
		goto insxit;

	  break;
	}

	// if key already exists, update value and return

	val = valptr(set->page, slot);

	if( val->len >= vallen ) {
	  if( node->dead )
	  	set->page->act++;
	  node->type = type;
	  node->dead = 0;

	  set->page->garbage += val->len - vallen;
	  val->len = vallen;
	  memcpy (val->value, value, vallen);
	  goto insxit;
	}

	//  new update value doesn't fit in existing value area
	//	make sure page has room

	if( !node->dead )
	  set->page->garbage += val->len + ptr->len + sizeof(BtKey) + sizeof(BtVal);
	else
	  set->page->act++;

	node->type = type;
	node->dead = 0;

	if( slot = bt_cleanpage (mgr, set, keylen, slot, vallen) )
	  break;

	if( entry = bt_splitpage (mgr, set, 1) )
	  if( !bt_splitkeys (mgr, set, entry + mgr->latchsets) )
		continue;

	return mgr->err;
  }

  //  copy key and value onto page and update slot

  set->page->min -= vallen + sizeof(BtVal);
  val = (BtVal*)((unsigned char *)set->page + set->page->min);
  memcpy (val->value, value, vallen);
  val->len = vallen;

  set->page->min -= keylen + sizeof(BtKey);
  ptr = (BtKey*)((unsigned char *)set->page + set->page->min);
  memcpy (ptr->key, key, keylen);
  ptr->len = keylen;
	
  slotptr(set->page,slot)->off = set->page->min;

insxit:
  bt_unlockpage(BtLockWrite, set->latch, __LINE__);
  bt_unpinlatch (set->latch);
  return 0;
}

//	determine actual page where key is located
//  return slot number

uint bt_atomicpage (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint idx, BtPageSet *set)
{
BtKey *key = keyptr(source,locks[idx].src), *ptr;
uint slot = locks[idx].slot;
uint entry;

	if( locks[idx].reuse )
	  entry = locks[idx-1].entry;
	else
	  entry = locks[idx].entry;

	if( slot ) {
		set->latch = mgr->latchsets + entry;
		set->page = bt_mappage (mgr, set->latch);
		return slot;
	}

	//	find where our key is located 
	//	on current page or pages split on
	//	same page txn operations.

	do {
		set->latch = mgr->latchsets + entry;
		set->page = bt_mappage (mgr, set->latch);

		if( slot = bt_findslot(set->page, key->key, key->len) ) {
		  if( slotptr(set->page, slot)->type == Librarian )
			slot++;
		  if( locks[idx].reuse )
			locks[idx].entry = entry;
		  return slot;
		}
	} while( entry = set->latch->split );

	mgr->line = __LINE__, mgr->err = BTERR_atomic;
	return 0;
}

BTERR bt_atomicinsert (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint idx)
{
BtKey *key = keyptr(source, locks[idx].src);
BtVal *val = valptr(source, locks[idx].src);
BtLatchSet *latch;
BtPageSet set[1];
uint entry, slot;

  while( slot = bt_atomicpage (mgr, source, locks, idx, set) ) {
	if( slot = bt_cleanpage(mgr, set, key->len, slot, val->len) ) {
	  if( bt_insertslot (mgr, set, slot, key->key, key->len, val->value, val->len, slotptr(source,locks[idx].src)->type) )
		return mgr->err;

	  return 0;
	}

	//  split page

	if( entry = bt_splitpage (mgr, set, 0) )
	  latch = mgr->latchsets + entry;
	else
	  return mgr->err;

	//	splice right page into split chain
	//	and WriteLock it

	bt_lockpage(BtLockWrite, latch, 0, __LINE__);
	latch->split = set->latch->split;
	set->latch->split = entry;

	// clear slot number for atomic page

	locks[idx].slot = 0;
  }

  return mgr->line = __LINE__, mgr->err = BTERR_atomic;
}

//	perform delete from smaller btree
//  insert a delete slot if not found there

BTERR bt_atomicdelete (BtMgr *mgr, BtPage source, AtomicTxn *locks, uint idx)
{
BtKey *key = keyptr(source, locks[idx].src);
BtLatchSet *latch;
uint slot, entry;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;

  while( slot = bt_atomicpage (mgr, source, locks, idx, set) ) {
	node = slotptr(set->page, slot);
	ptr = keyptr(set->page, slot);
	val = valptr(set->page, slot);

	//  if slot is not found on cache btree, insert a delete slot
	//	otherwise ignore the request.

	if( keycmp (ptr, key->key, key->len) )
	 if( !mgr->type )
	  if( slot = bt_cleanpage(mgr, set, key->len, slot, 0) )
		return bt_insertslot (mgr, set, slot, key->key, key->len, NULL, 0, Delete);
	  else { //  split page before inserting Delete slot
		if( entry = bt_splitpage (mgr, set, 0) )
		  latch = mgr->latchsets + entry;
		else
	      return mgr->err;

		//	splice right page into split chain
		//	and WriteLock it

		bt_lockpage(BtLockWrite, latch, 0, __LINE__);
		latch->split = set->latch->split;
		set->latch->split = entry;

		// clear slot number for atomic page

		locks[idx].slot = 0;
		continue;
	  }
	 else
	   return 0;

	//	if node is already dead,
	//	ignore the request.

	if( node->type == Delete || node->dead )
		return 0;

	//  if main LSM btree, delete the slot
	//	else change to delete type.

	if( mgr->type ) {
 		set->page->act--;
		node->dead = 1;
	} else
		node->type = Delete;

	__sync_fetch_and_add(&mgr->found, 1);
	return 0;
  }

  return mgr->line = __LINE__, mgr->err = BTERR_struct;
}

//	release master's splits from right to left

void bt_atomicrelease (BtMgr *mgr, uint entry)
{
BtLatchSet *latch = mgr->latchsets + entry;

	if( latch->split )
		bt_atomicrelease (mgr, latch->split);

	latch->split = 0;
	bt_unlockpage(BtLockWrite, latch, __LINE__);
	bt_unpinlatch(latch);
}

int qsortcmp (BtSlot *slot1, BtSlot *slot2, BtPage page)
{
BtKey *key1 = (BtKey *)((char *)page + slot1->off);
BtKey *key2 = (BtKey *)((char *)page + slot2->off);

	return keycmp (key1, key2->key, key2->len);
}
//	atomic modification of a batch of keys.

BTERR bt_atomictxn (BtDb *bt, BtPage source)
{
uint src, idx, slot, samepage, entry, que = 0;
BtKey *key, *ptr, *key2;
int result = 0;
BtSlot temp[1];
int type;

  // stable sort the list of keys into order to
  //	prevent deadlocks between threads.
/*
  for( src = 1; src++ < source->cnt; ) {
	*temp = *slotptr(source,src);
	key = keyptr (source,src);

	for( idx = src; --idx; ) {
	  key2 = keyptr (source,idx);
	  if( keycmp (key, key2->key, key2->len) < 0 ) {
		*slotptr(source,idx+1) = *slotptr(source,idx);
		*slotptr(source,idx) = *temp;
	  } else
		break;
	}
  }
*/
	qsort_r (slotptr(source,1), source->cnt, sizeof(BtSlot), (__compar_d_fn_t)qsortcmp, source);

  //  perform the individual actions in the transaction

  if( bt_atomicexec (bt->mgr, source, source->cnt, bt->tid) )
	return bt->mgr->err;

  // if number of active pages
  // is greater than the buffer pool
  // promote page into larger btree

  if( bt->main )
   if( bt->mgr->pagezero->leafpages > bt->mgr->maxleaves )
	if( bt_promote (bt) )
	  return bt->mgr->err;

  // return success

  return 0;
}

//	execute the source list of inserts/deletes

BTERR bt_atomicexec(BtMgr *mgr, BtPage source, uint count, pid_t tid)
{
uint slot, src, idx, samepage, entry, outidx;
BtPageSet set[1], prev[1];
unsigned char value[BtId];
BtLatchSet *latch;
uid right_page_no;
AtomicTxn *locks;
BtKey *key, *ptr;
BtPage page;
BtVal *val;

  locks = calloc (count, sizeof(AtomicTxn));
  memset (set, 0, sizeof(BtPageSet));
  outidx = 0;

  // Load the leaf page for each key
  // group same page references with reuse bit

  for( src = 0; src++ < count; ) {
	if( slotptr(source,src)->dead )
	  continue;

	key = keyptr(source, src);

	// first determine if this modification falls
	// on the same page as the previous modification
	//	note that the far right leaf page is a special case

	if( samepage = !!set->page )
	  samepage = !set->page->right || keycmp (ptr, key->key, key->len) >= 0;

	if( !samepage )
	  if( slot = bt_loadpage(mgr, set, key->key, key->len, 0, BtLockWrite, tid) )
		ptr = fenceptr(set->page), set->latch->split = 0;
	  else
  	 	return mgr->err;
	else
	  slot = 0;

	if( slot )
	 if( slotptr(set->page, slot)->type == Librarian )
	  slot++;

	entry = set->latch - mgr->latchsets;
	locks[outidx].reuse = samepage;
	locks[outidx].entry = entry;
	locks[outidx].slot = slot;
	locks[outidx].src = src;
	outidx++;
  }

  // insert or delete each key
  // process any splits or merges
  // run through txn list backwards

  samepage = outidx;

  for( src = outidx; src--; ) {
	if( locks[src].reuse )
	  continue;

	//  perform the txn operations
	//	from smaller to larger on
	//  the same page

	for( idx = src; idx < samepage; idx++ )
	 switch( slotptr(source,locks[idx].src)->type ) {
	 case Delete:
	  if( bt_atomicdelete (mgr, source, locks, idx) )
  	 	return mgr->err;
	  break;

	case Duplicate:
	case Unique:
	  if( bt_atomicinsert (mgr, source, locks, idx) )
  	 	return mgr->err;
	  break;

	default:
  	  bt_atomicpage (mgr, source, locks, idx, set);
	  break;
	}

	//	after the same page operations have finished,
	//  process master page for splits or deletion.

	latch = prev->latch = mgr->latchsets + locks[src].entry;
	prev->page = bt_mappage (mgr, prev->latch);
	samepage = src;

	//  pick-up all splits from master page
	//	each one is already pinned & WriteLocked.

	while( entry = prev->latch->split ) {
	  set->latch = mgr->latchsets + entry;
	  set->page = bt_mappage (mgr, set->latch);

	  // delete empty master page by undoing its split
	  //  (this is potentially another empty page)
	  //  note that there are no pointers to it yet

	  if( !prev->page->act ) {
		set->page->left = prev->page->left;
		memcpy (prev->page, set->page, mgr->page_size << mgr->leaf_xtra);
		bt_lockpage (BtLockDelete, set->latch, 0, __LINE__);
		bt_lockpage (BtLockLink, set->latch, 0, __LINE__);
		prev->latch->split = set->latch->split;
		bt_freepage (mgr, set);
		continue;
	  }

	  // remove empty split page from the split chain
	  // and return it to the free list. No other
	  // thread has its page number yet.

	  if( !set->page->act ) {
		prev->page->right = set->page->right;
		prev->latch->split = set->latch->split;

		bt_lockpage (BtLockDelete, set->latch, 0, __LINE__);
		bt_lockpage (BtLockLink, set->latch, 0, __LINE__);
		bt_freepage (mgr, set);
		continue;
	  }

	  //  update prev's fence key

	  ptr = fenceptr(prev->page);
	  bt_putid (value, prev->latch->page_no);

	  if( bt_insertkey (mgr, ptr->key, ptr->len, 1, value, BtId, Unique) )
		return mgr->err;

	  // splice in the left link into the split page

	  set->page->left = prev->latch->page_no;
	  *prev = *set;
	}

	//  update left pointer in next right page from last split page
	//	(if all splits were reversed or none occurred, latch->split == 0)

	if( latch->split ) {
	  //  fix left pointer in master's original (now split)
	  //  far right sibling or set rightmost page in page zero

	  if( right_page_no = prev->page->right ) {
		if( set->latch = bt_pinlatch (mgr, right_page_no) )
	  	  set->page = bt_mappage (mgr, set->latch);
	 	else
  	 	  return mgr->err;

	    bt_lockpage (BtLockLink, set->latch, 0, __LINE__);
	    set->page->left = prev->latch->page_no;
	    bt_unlockpage (BtLockLink, set->latch, __LINE__);
		bt_unpinlatch (set->latch);
	  } else {	// prev is rightmost page
	    bt_mutexlock (mgr->pagezero->lock);
		mgr->pagezero->rightleaf = prev->latch->page_no;
	    bt_releasemutex(mgr->pagezero->lock);
	  }

	  //  switch the original fence key from the
	  //  master page to the last split page.

	  ptr = fenceptr(prev->page);
	  bt_putid (value, prev->latch->page_no);

	  if( bt_insertkey (mgr, ptr->key, ptr->len, 1, value, BtId, Update) )
		return mgr->err;

	  //  unlock and unpin the split pages

	  bt_atomicrelease (mgr, latch->split);

	  //  unlock and unpin the master page

	  latch->split = 0;
	  bt_unlockpage(BtLockWrite, latch, __LINE__);
	  bt_unpinlatch(latch);
	  continue;
	}

	//	since there are no splits remaining, we're
	//  finished if master page occupied

	if( prev->page->act ) {
	  bt_unlockpage(BtLockWrite, prev->latch, __LINE__);
	  bt_unpinlatch(prev->latch);
	  continue;
	}

	// any and all splits were reversed, and the
	// master page located in prev is empty, delete it

	if( bt_deletepage (mgr, prev, 0) )
		return mgr->err;
  }

  // delete the slots

  for( idx = 0; idx++ < count; ) {
	if( slotptr(source,idx)->dead )
	  continue;

	slotptr(source,idx)->dead = 1;
	source->act--;
  }

  free (locks);
  return 0;
}

//  pick & promote a page into the larger btree

BTERR bt_promote (BtDb *bt)
{
BtPageSet set[1];
uint slot, idx;
BtSlot *node;
uid page_no;
BtKey *ptr;
BtVal *val;

  bt_mutexlock(bt->mgr->pagezero->promote);

  while( 1 ) {
	if( bt->mgr->pagezero->leafpromote < bt->mgr->pagezero->allocpage )
		page_no = bt->mgr->pagezero->leafpromote;
	else
		page_no = bt->mgr->pagezero->leaf_page;

	bt->mgr->pagezero->leafpromote = page_no + (1 << bt->mgr->leaf_xtra);

	if( page_no < bt->mgr->pagezero->leaf_page )
		continue;

	if( set->latch = bt_pinlatch (bt->mgr, page_no) )
		set->page = bt_mappage (bt->mgr,set->latch);

	//	skip upper level pages

	if( set->page->lvl ) {
	  set->latch->pin--;
	  bt_releasemutex(set->latch->modify);
	  continue;
	}

	if( !bt_mutextry(set->latch->modify) ) {
	  set->latch->pin--;
	  bt_releasemutex(set->latch->modify);
	  continue;
	}

	//  skip this page if it was pinned

	if( set->latch->pin > 1 ) {
	  set->latch->pin--;
	  bt_releasemutex(set->latch->modify);
	  continue;
	}

	// page has no right sibling

	if( !set->page->right ) {
	  set->latch->pin--;
	  bt_releasemutex(set->latch->modify);
	  continue;
	}

	// page is being killed or constructed

	if( set->page->nopromote || set->page->kill ) {
	  set->latch->pin--;
	  bt_releasemutex(set->latch->modify);
	  continue;
	}

	//	leave it locked for the
	//	duration of the promotion.

	bt_releasemutex(bt->mgr->pagezero->promote);
	bt_lockpage (BtLockWrite, set->latch, 0, __LINE__);
	bt_releasemutex(set->latch->modify);

	// transfer slots in our selected page to the main btree

if( !((page_no>>bt->mgr->leaf_xtra)%100) )
fprintf(stderr, "Promote page %lld, %d keys\n", page_no, set->page->act);

	if( bt_atomicexec (bt->main, set->page, set->page->cnt, bt->tid) ) {
		fprintf (stderr, "Promote error = %d line = %d\n", bt->main->err, bt->main->line);
		return bt->main->err;
	}

	//  now delete the page

	if( bt_deletepage (bt->mgr, set, 0) )
		fprintf (stderr, "Promote: delete page err = %d\n", bt->mgr->err);

	return bt->mgr->err;
  }
}

//	find unique key == given key, or first duplicate key in
//	leaf level and return number of value bytes
//	or (-1) if not found.

int bt_findkey (BtDb *bt, unsigned char *key, uint keylen, unsigned char *value, uint valmax)
{
int ret = -1, type;
BtPageSet set[1];
BtSlot *node;
BtKey *ptr;
BtVal *val;
uint slot;

 for( type = 0; type < 2; type++ )
  if( slot = bt_loadpage (type ? bt->main : bt->mgr, set, key, keylen, 0, BtLockRead, 0) ) {
	node = slotptr(set->page, slot);

	//	skip librarian slot place holder

	if( node->type == Librarian )
	  node = slotptr(set->page, ++slot);

	ptr = keyptr(set->page, slot);

	//	not there if we reach the stopper key
	//  or the key doesn't match what's on the page.

	if( slot == set->page->cnt )
	  if( !set->page->right ) {
		bt_unlockpage (BtLockRead, set->latch, __LINE__);
		bt_unpinlatch (set->latch);
		continue;
	}

	if( keycmp (ptr, key, keylen) ) {
		bt_unlockpage (BtLockRead, set->latch, __LINE__);
		bt_unpinlatch (set->latch);
		continue;
	}

	// key matches, return >= 0 value bytes copied
	//	or -1 if not there.

	if( node->type == Delete || node->dead ) {
		ret = -1;
		goto findxit;
	}

	val = valptr (set->page,slot);

	if( valmax > val->len )
		valmax = val->len;

	memcpy (value, val->value, valmax);
	ret = valmax;
	goto findxit;
  }

  ret = -1;

findxit:
  if( type < 2 ) {
	bt_unlockpage (BtLockRead, set->latch, __LINE__);
	bt_unpinlatch (set->latch);
  }
  return ret;
}

//	set cursor to highest slot on right-most page

BTERR bt_lastkey (BtDb *bt)
{
uid cache_page_no = bt->mgr->pagezero->rightleaf;
uid main_page_no = bt->main->pagezero->rightleaf;

	if( bt->cacheset->latch = bt_pinlatch (bt->mgr, cache_page_no) )
		bt->cacheset->page = bt_mappage (bt->mgr, bt->cacheset->latch);
	else
		return bt->mgr->err;

    bt_lockpage(BtLockRead, bt->cacheset->latch, 0, __LINE__);
	bt->cacheslot = bt->cacheset->page->cnt;

	if( bt->mainset->latch = bt_pinlatch (bt->main, main_page_no) )
		bt->mainset->page = bt_mappage (bt->main, bt->mainset->latch);
	else
		return bt->main->err;

    bt_lockpage(BtLockRead, bt->mainset->latch, 0, __LINE__);
	bt->mainslot = bt->mainset->page->cnt;
	bt->phase = 2;
	return 0;
}

//	return previous slot on cursor page

uint bt_prevslot (BtMgr *mgr, BtPageSet *set, uint slot)
{
uid next, us = set->latch->page_no;

  while( 1 ) {
	while( --slot )
	  if( slotptr(set->page, slot)->dead )
		continue;
	  else
		return slot;

	next = set->page->left;

	if( !next )
		return 0;

	do {
	  bt_unlockpage(BtLockRead, set->latch, __LINE__);
	  bt_unpinlatch (set->latch);

	  if( set->latch = bt_pinlatch (mgr, next) )
	    set->page = bt_mappage (mgr, set->latch);
	  else
	    return 0;

      bt_lockpage(BtLockRead, set->latch, 0, __LINE__);
	  next = set->page->right;

	} while( next != us );

    slot = set->page->cnt + 1;
  }
}

//  advance to previous key

BTERR bt_prevkey (BtDb *bt)
{
int cmp;

  // first advance last key(s) one previous slot

  while( 1 ) {
	switch( bt->phase ) {
	case 0:
	  bt->cacheslot = bt_prevslot (bt->mgr, bt->cacheset, bt->cacheslot);
	  break;
	case 1:
	  bt->mainslot = bt_prevslot (bt->main, bt->mainset, bt->mainslot);
	  break;
	case 2:
	  bt->cacheslot = bt_prevslot (bt->mgr, bt->cacheset, bt->cacheslot);
	  bt->mainslot = bt_prevslot (bt->main, bt->mainset, bt->mainslot);
	break;
	}

  // return next key

	if( bt->cacheslot ) {
	  bt->cachenode = slotptr(bt->cacheset->page, bt->cacheslot);
	  bt->cachekey = keyptr(bt->cacheset->page, bt->cacheslot);
	  bt->cacheval = valptr(bt->cacheset->page, bt->cacheslot);
	}

	if( bt->mainslot ) {
	  bt->mainnode = slotptr(bt->mainset->page, bt->mainslot);
	  bt->mainkey = keyptr(bt->mainset->page, bt->mainslot);
	  bt->mainval = valptr(bt->mainset->page, bt->mainslot);
	}

	if( bt->mainslot && bt->cacheslot )
	  cmp = keycmp (bt->cachekey, bt->mainkey->key, bt->mainkey->len);
	else if( bt->cacheslot )
	  cmp = 1;
	else if( bt->mainslot )
	  cmp = -1;
	else
	  return 0;

	//  cache key is larger

	if( cmp > 0 ) {
	  bt->phase = 0;
	  if( bt->cachenode->type == Delete )
		continue;
	  return bt->cacheslot;
	}

	//  main key is larger

	if( cmp < 0 ) {
	  bt->phase = 1;
	  return bt->mainslot;
	}

	//	keys are equal

	bt->phase = 2;

	if( bt->cachenode->type == Delete )
		continue;

	return bt->cacheslot;
  }
}

//	advance to next slot in cache or main btree
//	return 0 for EOF/error

uint bt_nextslot (BtMgr *mgr, BtPageSet *set, uint slot)
{
BtPage page;
uid page_no;

  while( 1 ) {
	while( slot++ < set->page->cnt )
	  if( slotptr(set->page, slot)->dead )
		continue;
	  else if( slot < set->page->cnt || set->page->right )
		return slot;
	  else
		return 0;

	bt_unlockpage(BtLockRead, set->latch, __LINE__);
	bt_unpinlatch (set->latch);

	if( page_no = set->page->right )
	  if( set->latch = bt_pinlatch (mgr, page_no) )
		set->page = bt_mappage (mgr, set->latch);
	  else
		return 0;
	else
	  return 0; // EOF

 	// obtain access lock using lock chaining with Access mode

	bt_lockpage(BtLockAccess, set->latch, 0, __LINE__);
	bt_lockpage(BtLockRead, set->latch, 0, __LINE__);
	bt_unlockpage(BtLockAccess, set->latch, __LINE__);
	slot = 0;
  }
}

//  advance to next key

BTERR bt_nextkey (BtDb *bt)
{
int cmp;

  // first advance last key(s) one next slot

  while( 1 ) {
	switch( bt->phase ) {
	case 0:
	  bt->cacheslot = bt_nextslot (bt->mgr, bt->cacheset, bt->cacheslot);
	  break;
	case 1:
	  bt->mainslot = bt_nextslot (bt->main, bt->mainset, bt->mainslot);
	  break;
	case 2:
	  bt->cacheslot = bt_nextslot (bt->mgr, bt->cacheset, bt->cacheslot);
	  bt->mainslot = bt_nextslot (bt->main, bt->mainset, bt->mainslot);
	break;
	}

  // return next key

	if( bt->cacheslot ) {
	  bt->cachenode = slotptr(bt->cacheset->page, bt->cacheslot);
	  bt->cachekey = keyptr(bt->cacheset->page, bt->cacheslot);
	  bt->cacheval = valptr(bt->cacheset->page, bt->cacheslot);
	}

	if( bt->mainslot ) {
	  bt->mainnode = slotptr(bt->mainset->page, bt->mainslot);
	  bt->mainkey = keyptr(bt->mainset->page, bt->mainslot);
	  bt->mainval = valptr(bt->mainset->page, bt->mainslot);
	}

	if( bt->mainslot && bt->cacheslot )
	  cmp = keycmp (bt->cachekey, bt->mainkey->key, bt->mainkey->len);
	else if( bt->mainslot )
	  cmp = 1;
	else if( bt->cacheslot )
	  cmp = -1;
	else
	  return 0;

	//  main key is larger
	//	return smaller key

	if( cmp < 0 ) {
	  bt->phase = 0;
	  if( bt->cachenode->type == Delete )
		continue;
	  return bt->cacheslot;
	}

	//  cache key is larger

	if( cmp > 0 ) {
	  bt->phase = 1;
	  return bt->mainslot;
	}

	//	keys are equal

	bt->phase = 2;

	if( bt->cachenode->type == Delete )
		continue;

	return bt->cacheslot;
  }
}

//  start sweep of keys

BTERR bt_startkey (BtDb *bt, unsigned char *key, uint len)
{
BtPageSet set[1];
uint slot;

	// cache btree page

	if( slot = bt_loadpage (bt->mgr, bt->cacheset, key, len, 0, BtLockRead, 0) )
		bt->cacheslot = slot - 1;
	else
	  return bt->mgr->err;

	// main btree page

	if( slot = bt_loadpage (bt->main, bt->mainset, key, len, 0, BtLockRead, 0) )
		bt->mainslot = slot - 1;
	else
	  return bt->mgr->err;

	bt->phase = 2;
	return 0;
}

//	flush cache pages to main btree

BTERR bt_flushmain (BtDb *bt)
{
uint count, cnt = 0;
BtPageSet set[1];

  while( bt->mgr->pagezero->leafpages > 0 ) {
	if( set->latch = bt_pinlatch (bt->mgr, bt->mgr->pagezero->leaf_page) )
	  set->page = bt_mappage (bt->mgr, set->latch);
	else
	  return bt->mgr->err;

	bt_lockpage(BtLockWrite, set->latch, 0, __LINE__);
	count = set->page->cnt;

	if( !set->page->right )
		count--;

if( !(cnt++ % 100) )
fprintf(stderr, "Promote LEAF_page %d with %d keys\n", cnt, set->page->act);

	if( bt_atomicexec (bt->main, set->page, count, bt->tid) )
	  return bt->mgr->line = bt->main->line, bt->mgr->err = bt->main->err;

	if( set->page->right )
	  if( bt_deletepage (bt->mgr, set, 0) )
		return bt->mgr->err;
	  else
		continue;

	bt_unlockpage(BtLockWrite, set->latch, __LINE__);
	bt_unpinlatch (set->latch);
	return 0;
  }

  //  leaf page count is off

  bt->mgr->line = __LINE__;
  return bt->mgr->err = BTERR_ovflw;
}

#ifdef STANDALONE

#ifndef unix
double getCpuTime(int type)
{
FILETIME crtime[1];
FILETIME xittime[1];
FILETIME systime[1];
FILETIME usrtime[1];
SYSTEMTIME timeconv[1];
double ans = 0;

	memset (timeconv, 0, sizeof(SYSTEMTIME));

	switch( type ) {
	case 0:
		GetSystemTimeAsFileTime (xittime);
		FileTimeToSystemTime (xittime, timeconv);
		ans = (double)timeconv->wDayOfWeek * 3600 * 24;
		break;
	case 1:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (usrtime, timeconv);
		break;
	case 2:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (systime, timeconv);
		break;
	}

	ans += (double)timeconv->wHour * 3600;
	ans += (double)timeconv->wMinute * 60;
	ans += (double)timeconv->wSecond;
	ans += (double)timeconv->wMilliseconds / 1000;
	return ans;
}
#else
#include <time.h>
#include <sys/resource.h>

double getCpuTime(int type)
{
struct rusage used[1];
struct timeval tv[1];

	switch( type ) {
	case 0:
		gettimeofday(tv, NULL);
		return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;

	case 1:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;

	case 2:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
	}

	return 0;
}
#endif

void bt_poolaudit (BtMgr *mgr, char *type)
{
BtLatchSet *latch, test[1];
uint entry;

	memset (test, 0, sizeof(test));

	if( memcmp (test, mgr->latchsets, sizeof(test)) )
		fprintf(stderr, "%s latchset zero overwritten\n", type);

	for( entry = 0; ++entry < mgr->pagezero->latchtotal; ) {
		latch = mgr->latchsets + entry;

		if( *latch->modify->value )
			fprintf(stderr, "%s latchset %d modifylocked for page %lld\n", type, entry, latch->page_no);

		if( latch->pin )
			fprintf(stderr, "%s latchset %d pinned %d times for page %lld\n", type, entry, latch->pin, latch->page_no);
	}
}

typedef struct {
	char idx;
	char *type;
	char *infile;
	BtMgr *main;
	BtMgr *mgr;
	int num;
} ThreadArg;

//  standalone program to index file of keys
//  then list them onto std-out

#ifdef unix
void *index_file (void *arg)
#else
uint __stdcall index_file (void *arg)
#endif
{
int line = 0, found = 0, cnt = 0, cachecnt, idx;
int ch, len = 0, slot, type = 0;
unsigned char key[BT_maxkey];
unsigned char buff[65536];
uint nxt = sizeof(buff);
ThreadArg *args = arg;
uint counts[8][2];
BtPageSet set[1];
BtPage page;
uid page_no;
int vallen;
BtKey *ptr;
BtVal *val;
uint size;
BtDb *bt;
FILE *in;

	bt = bt_open (args->mgr, args->main);
	page = (BtPage)buff;

	if( args->idx < strlen (args->type) )
		ch = args->type[args->idx];
	else
		ch = args->type[strlen(args->type) - 1];

	switch(ch | 0x20)
	{
	case 'a':
	  bt_poolaudit(bt->mgr, "cache");
	  bt_poolaudit(bt->main, "main");
	  break;

	case 'm':
	  fprintf(stderr, "started flushing cache to main btree\n");

  	  if( bt->main )
		if( bt_flushmain(bt) )
		  fprintf(stderr, "Error %d Line: %d\n", bt->mgr->err, bt->mgr->line), exit(0);

	  break;

	case 'd':
		type = Delete;

	case 'p':
		if( !type )
			type = Unique;

		if( args->num )
		 if( type == Delete )
		  fprintf(stderr, "started TXN pennysort delete for %s\n", args->infile);
		 else
		  fprintf(stderr, "started TXN pennysort insert for %s\n", args->infile);
		else
		 if( type == Delete )
		  fprintf(stderr, "started pennysort delete for %s\n", args->infile);
		 else
		  fprintf(stderr, "started pennysort insert for %s\n", args->infile);

		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  if( !args->num ) {
			    if( bt_insertkey (bt->mgr, key, 10, 0, key + 10, len - 10, Unique) )
				  fprintf(stderr, "Error %d Line: %d source: %d\n", bt->mgr->err, bt->mgr->line, line), exit(0);
			    len = 0;
				continue;
			  }

			  nxt -= len - 10;
			  memcpy (buff + nxt, key + 10, len - 10);
			  nxt -= 1;
			  buff[nxt] = len - 10;
			  nxt -= 10;
			  memcpy (buff + nxt, key, 10);
			  nxt -= 1;
			  buff[nxt] = 10;
			  slotptr(page,++cnt)->off  = nxt;
			  slotptr(page,cnt)->type = type;
			  slotptr(page,cnt)->dead = 0;
			  len = 0;

			  if( cnt < args->num )
				continue;

			  page->cnt = cnt;
			  page->act = cnt;
			  page->min = nxt;

			  if( bt_atomictxn (bt, page) )
				fprintf(stderr, "Error %d Line: %d source: %d\n", bt->mgr->err, bt->mgr->line, line), exit(0);
			  nxt = sizeof(buff);
			  cnt = 0;

			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		break;

	case 'w':
		fprintf(stderr, "started indexing for %s\n", args->infile);
		if( in = fopen (args->infile, "r") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  if( bt_insertkey (bt->mgr, key, len, 0, NULL, 0, Unique) )
				fprintf(stderr, "Error %d Line: %d source: %d\n", bt->mgr->err, bt->mgr->line, line), exit(0);
			  len = 0;
			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;
			  if( bt_findkey (bt, key, len, NULL, 0) == 0 )
				found++;
			  else if( bt->mgr->err )
				fprintf(stderr, "Error %d Syserr %d Line: %d source: %d\n", bt->mgr->err, errno, bt->mgr->line, line), exit(0);
			  len = 0;
			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys, found %d\n", args->infile, line, found);
		break;

	case 's':
		fprintf(stderr, "started forward scan\n");
		if( bt_startkey (bt, NULL, 0) )
			fprintf(stderr, "unable to begin scan error %d Line: %d\n", bt->mgr->err, bt->mgr->line);

		while( bt_nextkey (bt) ) {
		  if( bt->phase == 1 ) {
			len = bt->mainkey->len;

			if( bt->mainnode->type == Duplicate )
				len -= BtId;

			fwrite (bt->mainkey->key, len, 1, stdout);
			fwrite (bt->mainval->value, bt->mainval->len, 1, stdout);
		  } else {
			len = bt->cachekey->len;

			if( bt->cachenode->type == Duplicate )
				len -= BtId;

			fwrite (bt->cachekey->key, len, 1, stdout);
			fwrite (bt->cacheval->value, bt->cacheval->len, 1, stdout);
		  }

		  fputc ('\n', stdout);
		  cnt++;
	    }

		bt_unlockpage(BtLockRead, bt->cacheset->latch, __LINE__);
		bt_unpinlatch (bt->cacheset->latch);

		bt_unlockpage(BtLockRead, bt->mainset->latch, __LINE__);
		bt_unpinlatch (bt->mainset->latch);

		fprintf(stderr, " Total keys read %d\n", cnt);
		break;

	case 'r':
		fprintf(stderr, "started reverse scan\n");
		if( bt_lastkey (bt) )
			fprintf(stderr, "unable to begin scan error %d Line: %d\n", bt->mgr->err, bt->mgr->line);

		while( bt_prevkey (bt) ) {
		  if( bt->phase == 1 ) {
			len = bt->mainkey->len;

			if( bt->mainnode->type == Duplicate )
				len -= BtId;

			fwrite (bt->mainkey->key, len, 1, stdout);
			fwrite (bt->mainval->value, bt->mainval->len, 1, stdout);
		  } else {
			len = bt->cachekey->len;

			if( bt->cachenode->type == Duplicate )
				len -= BtId;

			fwrite (bt->cachekey->key, len, 1, stdout);
			fwrite (bt->cacheval->value, bt->cacheval->len, 1, stdout);
		  }

		  fputc ('\n', stdout);
		  cnt++;
	    }

		bt_unlockpage(BtLockRead, bt->cacheset->latch, __LINE__);
		bt_unpinlatch (bt->cacheset->latch);

		bt_unlockpage(BtLockRead, bt->mainset->latch, __LINE__);
		bt_unpinlatch (bt->mainset->latch);

		fprintf(stderr, " Total keys read %d\n", cnt);
		break;

	case 'c':
		fprintf(stderr, "started counting LSM cache btree\n");
		memset (counts, 0, sizeof(counts));
		page_no = bt->mgr->pagezero->leaf_page;

		size = bt->mgr->page_size << bt->mgr->leaf_xtra;
		page = malloc(size);

#ifdef unix
		posix_fadvise( bt->mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
		while( page_no < bt->mgr->pagezero->allocpage ) {
		  if( bt_readpage (bt->mgr, page, page_no, 0) )
			fprintf(stderr, "Unable to read page %lld from cache\n", page_no), exit(1);
		  if( !page->lvl && !page->free ) {
		    cnt += page->act;

			for( idx = 0; idx++ < page->cnt; ) {
			BtSlot *node = slotptr (page, idx);
			  counts[node->type][node->dead]++;
			}
		  }
		  page_no += 1 << bt->mgr->leaf_xtra;
		}
		
	  	cachecnt = --cnt;	// remove stopper key
		counts[Unique][0]--;

		fprintf(stderr, "  Unique    : %d  dead: %d\n", counts[Unique][0], counts[Unique][1]);
		fprintf(stderr, "  Duplicates: %d  dead: %d\n", counts[Duplicate][0], counts[Duplicate][1]);
		fprintf(stderr, "  Librarian : %d  dead: %d\n", counts[Librarian][0], counts[Librarian][1]);
		fprintf(stderr, "  Deletion  : %d  dead: %d\n", counts[Delete][0], counts[Delete][1]);
		fprintf(stderr, "total cache keys count: %d\n", cachecnt);
		free (page);

		fprintf(stderr, "started counting LSM main btree\n");
		memset (counts, 0, sizeof(counts));
		size = bt->main->page_size << bt->main->leaf_xtra;
		page_no = bt->mgr->pagezero->leaf_page;
		page = malloc(size);
		cnt = 0;

#ifdef unix
		posix_fadvise( bt->main->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
		while( page_no < bt->main->pagezero->allocpage ) {
		  if( bt_readpage (bt->main, page, page_no, 0) )
			fprintf(stderr, "Unable to read page %lld from main\n", page_no), exit(1);
		  if( !page->lvl && !page->free ) {
		    cnt += page->act;

			for( idx = 0; idx++ < page->cnt; ) {
			BtSlot *node = slotptr (page, idx);
			  counts[node->type][node->dead]++;
			}
		  }
		  page_no += 1 << bt->main->leaf_xtra;
		}
		
	  	cnt--;	// remove stopper key
		counts[Unique][0]--;

		fprintf(stderr, "  Unique    : %d  dead: %d\n", counts[Unique][0], counts[Unique][1]);
		fprintf(stderr, "  Duplicates: %d  dead: %d\n", counts[Duplicate][0], counts[Duplicate][1]);
		fprintf(stderr, "  Librarian : %d  dead: %d\n", counts[Librarian][0], counts[Librarian][1]);
		fprintf(stderr, "  Deletion  : %d  dead: %d\n", counts[Delete][0], counts[Delete][1]);
		fprintf(stderr, "total main keys count : %d\n", cnt);
		fprintf(stderr, "Total keys counted    : %d\n", cnt + cachecnt);
		free (page);
		break;
	}

	bt_close (bt);
#ifdef unix
	return NULL;
#else
	return 0;
#endif
}

typedef struct timeval timer;

int main (int argc, char **argv)
{
int idx, cnt, len, slot, err;
double start, stop;
#ifdef unix
pthread_t *threads;
#else
HANDLE *threads;
#endif
ThreadArg *args;
uint mainleafpool = 0;
uint mainleafxtra = 0;
uint maxleaves = 0;
uint poolsize = 0;
uint leafpool = 0;
uint leafxtra = 0;
uint mainpool = 0;
uint mainbits = 0;
int bits = 16;
float elapsed;
int num = 0;
char key[1];
BtMgr *main;
BtMgr *mgr;
BtKey *ptr;

	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s idx_file main_file cmds [pagebits leafbits poolsize txnsize mainbits mainleafbits mainpool maxleaves src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where idx_file is the name of the cache btree file\n");
		fprintf (stderr, "  where main_file is the name of the main btree file\n");
		fprintf (stderr, "  cmds is a string of (r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennysort/(c)ount/(m)ainflush/(a)udit, with a one character command for each input src_file. A command can also be given with no input file\n");
		fprintf (stderr, "  pagebits is the page size in bits for the cache btree\n");
		fprintf (stderr, "  leafbits is the number of xtra bits for a leaf page\n");
		fprintf (stderr, "  poolsize is the number of latches in latch pool for the cache btree\n");
		fprintf (stderr, "  txnsize = n to block transactions into n units, or zero for no transactions\n");
		fprintf (stderr, "  mainbits is the page size of the main btree in bits\n");
		fprintf (stderr, "  mainpool is the number of latches in the main latch pool\n");
		fprintf (stderr, "  maxleaves is the threashold for LSM leaf page promotion\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys separated by newline\n");
		exit(0);
	}

	start = getCpuTime(0);

	if( argc > 4 )
		bits = atoi(argv[4]);

	if( argc > 5 )
		leafxtra = atoi(argv[5]);

	if( argc > 6 )
		poolsize = atoi(argv[6]);

	if( argc > 7 )
		num = atoi(argv[7]);

	if( argc > 8 )
		mainbits = atoi(argv[8]);

	if( argc > 9 )
		mainleafxtra = atoi(argv[9]);

	if( argc > 10 )
		mainpool = atoi(argv[10]);

	if( argc > 11 )
		maxleaves = atoi(argv[11]);

	if( argc > 12 )
		cnt = argc - 12;
	else
		cnt = 0;

#ifdef unix
	threads = malloc (cnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
	args = malloc ((cnt + 1) * sizeof(ThreadArg));

	mgr = bt_mgr (argv[1], bits, leafxtra, poolsize);

	if( !mgr ) {
		fprintf(stderr, "Index Open Error %s\n", argv[1]);
		exit (1);
	} else {
		mgr->maxleaves = maxleaves;
		mgr->type = 0;
	}

	main = bt_mgr (argv[2], mainbits, mainleafxtra, mainpool);

	if( !main ) {
		fprintf(stderr, "Index Open Error %s\n", argv[2]);
		exit (1);
	} else
		main->type = 1;

	//	fire off threads

	if( cnt > 1 )
	  for( idx = 0; idx < cnt; idx++ ) {
		args[idx].infile = argv[idx + 12];
		args[idx].type = argv[3];
		args[idx].main = main;
		args[idx].mgr = mgr;
		args[idx].num = num;
		args[idx].idx = idx;
#ifdef unix
		if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
			fprintf(stderr, "Error creating thread %d\n", err);
#else
		threads[idx] = (HANDLE)_beginthreadex(NULL, 131072, index_file, args + idx, 0, NULL);
#endif
	  }
	else {
		args[0].infile = argv[12];
		args[0].type = argv[3];
		args[0].main = main;
		args[0].mgr = mgr;
		args[0].num = num;
		args[0].idx = 0;
		index_file (args);
	}

	// 	wait for termination

#ifdef unix
	if( cnt > 1 )
	  for( idx = 0; idx < cnt; idx++ )
		pthread_join (threads[idx], NULL);
#else
	if( cnt > 1 )
	  WaitForMultipleObjects (cnt, threads, TRUE, INFINITE);

	if( cnt > 1 )
	  for( idx = 0; idx < cnt; idx++ )
		CloseHandle(threads[idx]);
#endif
	fprintf(stderr, "cache %lld leaves %lld upper %d found\n", mgr->pagezero->leafpages, mgr->pagezero->upperpages, mgr->found);
	if( main )
		fprintf(stderr, "main %lld leaves %lld upper %d found\n", main->pagezero->leafpages, main->pagezero->upperpages, main->found);

	bt_mgrclose (mgr);

	if( main )
		bt_mgrclose (main);

	elapsed = getCpuTime(0) - start;
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
}

BtKey *bt_fence (BtPage page)
{
return fenceptr(page);
}

BtKey *bt_key (BtPage page, uint slot)
{
return keyptr(page,slot);
}

BtSlot *bt_slot (BtPage page, uint slot)
{
return slotptr(page,slot);
}
#endif	//STANDALONE
