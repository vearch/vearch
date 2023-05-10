#ifdef __APPLE__
#define unix
#endif


typedef unsigned long long	uid;

// #ifndef unix
typedef unsigned long long	off64_t;
typedef unsigned short		ushort;
typedef unsigned int		uint;
// #endif

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw

#define BT_maxbits		24					// maximum page size in bits
#define BT_minbits		9					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size
#define BT_maxpage		(1 << BT_maxbits)	// maximum page size

//  BTree page number constants
#define ALLOC_page		0	// allocation page
#define ROOT_page		1	// root of the btree
#define LEAF_page		2	// first page of leaves

//	Number of levels to create in a new BTree

#define MIN_lvl			2

/*
There are six lock types for each node in four independent sets: 
1. (set 1) AccessIntent: Sharable. Going to Read the node. Incompatible with NodeDelete. 
2. (set 1) NodeDelete: Exclusive. About to release the node. Incompatible with AccessIntent. 
3. (set 2) ReadLock: Sharable. Read the node. Incompatible with WriteLock. 
4. (set 2) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and other WriteLocks. 
5. (set 3) ParentModification: Exclusive. Change the node's parent keys. Incompatible with another ParentModification. 
6. (set 4) AtomicModification: Exclusive. Atomic Update including node is underway. Incompatible with another AtomicModification. 
*/

typedef enum{
	BtLockAccess = 1,
	BtLockDelete = 2,
	BtLockRead   = 4,
	BtLockWrite  = 8,
	BtLockParent = 16,
	BtLockAtomic = 32
} BtLock;

//	definition for phase-fair reader/writer lock implementation

typedef struct {
	ushort rin[1];
	ushort rout[1];
	ushort ticket[1];
	ushort serving[1];
} RWLock;

#define PHID 0x1
#define PRES 0x2
#define MASK 0x3
#define RINC 0x4

//	definition for spin latch implementation

// exclusive is set for write access
// share is count of read accessors
// grant write lock when share == 0

volatile typedef struct {
	ushort exclusive:1;
	ushort pending:1;
	ushort share:14;
} BtSpinLatch;

#define XCL 1
#define PEND 2
#define BOTH 3
#define SHARE 4

//	write only reentrant lock

typedef struct {
	BtSpinLatch xcl[1];
	volatile ushort tid[1];
	volatile ushort dup[1];
} WOLock;

//  hash table entries

typedef struct {
	volatile uint slot;		// Latch table entry at head of chain
	BtSpinLatch latch[1];
} BtHashEntry;

//	latch manager table structure

typedef struct {
	uid page_no;			// latch set page number
	RWLock readwr[1];		// read/write page lock
	RWLock access[1];		// Access Intent/Page delete
	WOLock parent[1];		// Posting of fence key in parent
	WOLock atomic[1];		// Atomic update in progress
	uint split;				// right split page atomic insert
	uint entry;				// entry slot in latch table
	uint next;				// next entry in hash table chain
	uint prev;				// prev entry in hash table chain
	volatile ushort pin;	// number of outstanding threads
	ushort dirty:1;			// page in cache is dirty
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
	Librarian,
	Duplicate,
	Delete,
	Update
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

//	note that this structure size
//	must be a multiple of 8 bytes
//	in order to place dups correctly.

typedef struct BtPage_ {
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	uint garbage;				// page garbage in bytes
	unsigned char bits:7;		// page size in bits
	unsigned char free:1;		// page is on free chain
	unsigned char lvl:7;		// level of page
	unsigned char kill:1;		// page is being deleted
	unsigned char right[BtId];	// page number to right
	unsigned char left[BtId];	// page number to left
	unsigned char filler[2];	// padding to multiple of 8
} *BtPage;

//  The loadpage interface object

typedef struct {
	BtPage page;		// current page pointer
	BtLatchSet *latch;	// current page latch set
} BtPageSet;

//	structure for latch manager on ALLOC_page

typedef struct {
	struct BtPage_ alloc[1];	// next page_no in right ptr
	unsigned long long dups[1];	// global duplicate key uniqueifier
	unsigned char chain[BtId];	// head of free page_nos chain
} BtPageZero;

//	The object structure for Btree access

typedef struct {
	uint page_size;				// page size	
	uint page_bits;				// page size in bits	
#ifdef unix
	int idx;
#else
	HANDLE idx;
#endif
	BtPageZero *pagezero;		// mapped allocation page
	BtSpinLatch lock[1];		// allocation area lite latch
	uint latchdeployed;			// highest number of latch entries deployed
	uint nlatchpage;			// number of latch pages at BT_latch
	uint latchtotal;			// number of page latch entries
	uint latchhash;				// number of latch hash table slots
	uint latchvictim;			// next latch entry to examine
	ushort thread_no[1];		// next thread number
	BtHashEntry *hashtable;		// the buffer pool hash table entries
	BtLatchSet *latchsets;		// mapped latch set from buffer pool
	unsigned char *pagepool;	// mapped to the buffer pool pages
#ifndef unix
	HANDLE halloc;				// allocation handle
	HANDLE hpool;				// buffer pool handle
#endif
} BtMgr;

typedef struct {
	BtMgr *mgr;				// buffer manager for thread
	BtPage cursor;			// cached frame for start/next (never mapped)
	BtPage frame;			// spare frame for the page split (never mapped)
	uid cursor_page;				// current cursor page number	
	unsigned char *mem;				// frame, cursor, page memory buffer
	unsigned char key[BT_keyarray];	// last found complete key
	int found;				// last delete or insert was found
	int err;				// last error
	int reads, writes;		// number of reads and writes from the btree
	ushort thread_no;		// thread number
} BtDb;

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

#define CLOCK_bit 0x8000

#ifdef __cplusplus
extern "C" {
#endif

// B-Tree functions
extern void bt_close (BtDb *bt);
extern BtDb *bt_open (BtMgr *mgr);
extern BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, void *value, uint vallen, uint update);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl);
extern int bt_findkey    (BtDb *bt, unsigned char *key, uint keylen, unsigned char *value, uint valmax);
extern BtKey *bt_foundkey (BtDb *bt);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//	manager functions
extern BtMgr *bt_mgr (char *name, uint bits, uint poolsize);
void bt_mgrclose (BtMgr *mgr);

//  Helper functions to return slot values
//	from the cursor page.

extern BtKey *bt_key (BtDb *bt, uint slot);
extern BtVal *bt_val (BtDb *bt, uint slot);

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

#define slotptr(page, slot) (((BtSlot *)(page+1)) + (slot-1))
#define keyptr(page, slot) ((BtKey*)((unsigned char*)(page) + slotptr(page, slot)->off))
#define valptr(page, slot) ((BtVal*)(keyptr(page,slot)->key + keyptr(page,slot)->len))

int keycmp (BtKey* key1, unsigned char *key2, uint len2);
BtLatchSet *bt_pinlatch (BtDb *bt, uid page_no, uint loadit);
BtPage bt_mappage (BtDb *bt, BtLatchSet *latch);
void bt_lockpage(BtDb *bt, BtLock mode, BtLatchSet *latch);
uid bt_getid(unsigned char *src);    
void bt_unlockpage(BtDb *bt, BtLock mode, BtLatchSet *latch);
void bt_unpinlatch (BtLatchSet *latch); 

#ifdef __cplusplus
}
#endif