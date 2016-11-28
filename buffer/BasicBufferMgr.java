package simpledb.buffer;

import java.util.LinkedHashMap;
import java.util.Map;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	// linked hash map
   private Map<Block, Buffer> bufferPoolMap;
	
   private int numAvailable;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferPoolMap = new LinkedHashMap<>();
      numAvailable = numbuffs;
      //for (int i=0; i<numbuffs; i++)
         //numbuffs = bufferpoolMap.size();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
	   for(Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()){
		   Buffer buff = entry.getValue();
         if (buff.isModifiedBy(txnum))
         buff.flush();
      }
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
	   Buffer buff = findExistingBuffer(blk);
	      if (buff == null) {
	    	 if(numAvailable==0){
	    		  buff = chooseUnpinnedBuffer();
	    		  if (buff == null)
	    			  return null;
	    		  if(buff.block()!=null)
	    			  bufferPoolMap.remove(buff.block());		//remove existing mapping
	    		  buff.assignToBlock(blk);
	    	 }
	      }
	      if (!buff.isPinned())
	         numAvailable--;
	      bufferPoolMap.put(blk, buff);						//add new mapping
	      buff.pin();
	      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      bufferPoolMap.put(buff.block(), buff);						//add new mapping
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
	   //refer key of hashmap no need of loop
	   if(bufferPoolMap.containsKey(blk)) {
		   Buffer buff = bufferPoolMap.get(blk);
		   return buff;
	   }
	   
	   return null;
      /*for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;*/
   }
   
   private Buffer chooseUnpinnedBuffer() {
	   // iterate over keys of linked hash map
	   	
	   for(Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()){
		   Buffer buff = entry.getValue();
		   if (!buff.isPinned())
			   return buff;
      }
      return null;
   }
}
