#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* file = nullptr; // Initialize file pointer
    Status status;
    FileHdrPage* hdrPage = nullptr;
    int hdrPageNo;
    int dataPageNo;
    Page* dataPage = nullptr;

    // Attempt to open the file
    status = db.openFile(fileName, file);
    if (status == OK) {
        // File already exists
        return FILEEXISTS;
    }

    // Create the file and reopen it
    status = db.createFile(fileName);
    if (status != OK) {
        return status; // Return failure status if file creation fails
    }
    status = db.openFile(fileName, file);
    if (status != OK) {
        return status; // Return failure status if file opening fails
    }

    // Allocate the header page
    status = bufMgr->allocPage(file, hdrPageNo, reinterpret_cast<Page*&>(hdrPage));
    if (status != OK) {
        return status; // Return failure status if allocation fails
    }

    // Initialize header page fields
    strcpy(hdrPage->fileName, fileName.c_str());
    hdrPage->pageCnt = 1; // Start with the header page
    hdrPage->recCnt = 0;

    // Allocate the first data page
    status = bufMgr->allocPage(file, dataPageNo, dataPage);
    if (status != OK) {
        bufMgr->unPinPage(file, hdrPageNo, false); // Unpin the header page
        return status; // Return failure status if allocation fails
    }
    dataPage->init(dataPageNo);

    // Update header page with first and last page numbers
    cout << "first page is hdr page: " << dataPageNo << endl;

    hdrPage->firstPage = dataPageNo;
    hdrPage->lastPage = dataPageNo;

    // Unpin and mark both pages as dirty
    bufMgr->unPinPage(file, hdrPageNo, true);
    bufMgr->unPinPage(file, dataPageNo, true);

    return OK; // Return success
}


// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;
    
    // open the file and read in the header page and the first data page
    status = db.openFile(fileName, filePtr);
    if (status == OK)
    {
		headerPageNo = 1; // first we need to get the header page (always assumed to be page No 1)
        returnStatus= bufMgr->readPage(filePtr, headerPageNo, curPage);
        headerPage = reinterpret_cast<FileHdrPage*>(curPage);
        

        if (returnStatus != OK) {
            cerr << "HeapFile: Failed to read header page for file " << fileName << endl;
            db.closeFile(filePtr); // Clean up
            return;
        }
        
        // Initialize member variables
        curPage = nullptr;
        curPageNo = 0;
        curDirtyFlag = false;
        hdrDirtyFlag = false;

        returnStatus = OK;
        return;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;
    
    // If record is on a different page, unpin current page and load the required page
    if (rid.pageNo != curPageNo) {
        if (curPage) {
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        } 

        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) return status;

        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }

    // Retrieve the record from the current page
    curPage->getRecord(rid, rec);
    return status;  
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    while(true)
    {
        status = curPage->nextRecord(tmpRid, nextRid);
        if (status == ENDOFPAGE) {
            // Move to the next page if available
            status = curPage->getNextPage(nextPageNo);
            if (status != OK) return status;

            // Unpin current page and load the next page
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if (status != OK) return status;

            curPageNo = nextPageNo;
            curDirtyFlag = false;
            curRec = RID();
            continue;
        }	
        if (status != OK) return status;
        // Apply filter if defined
        status = curPage->getRecord(outRid, rec);
        if (status != OK) return status;
        if (matchRec(rec)) return OK;
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // If no current page, read the last page into the buffer
    if (curPage == nullptr) {
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            cerr << "Error in reading the last page.\n";
            return status;
        }
    }

    // Try to insert the record into the current page
    status = curPage->insertRecord(rec, rid);
    if (status == NOSPACE) {
        // Current page is full, create a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);

        if (status != OK) {
            cerr << status << endl;
            cerr << "Error in allocating new page.\n";
            return status;
        }

        // Initialize the new page
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);

        newPage->init(newPageNo);
        curPageNo = newPageNo;
        curPage = newPage;
        curDirtyFlag = true;

        // Link the new page to the file
        status = curPage->setNextPage(headerPage->lastPage);
        if (status != OK) {
            cerr << "Error linking new page to the file.\n";
            return status;
        }

        headerPage->lastPage = newPageNo;
        headerPage->pageCnt += 1;
        hdrDirtyFlag = true;

        // Try inserting the record into the new page
        status = curPage->insertRecord(rec, rid);
        if (status != OK) {
            cerr << "Error inserting record into the new page.\n";
            return status;
        }
    } else if (status != OK) {
        cerr << "Error inserting record into the current page.\n";
        return status;
    }

    // Update bookkeeping
    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;

    // Update output RID
    outRid = rid;

    return OK;
}


