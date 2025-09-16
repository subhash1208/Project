package com.adp.datacloud.ds.udafs
  
/**
 * A mutable array implementation backed by underlying java Array object. 
 * This class provides a similar functionality as ArrayBuffer, 
 * but avoids doing a "toArray" call during serialization. 
 * The toArray call in ArrayBuffer is quite inefficient because of array copy being done under the hood 
 * 
 * Case class objects are by default serializable.
 *     
 */
//class WrappedArrayBuffer[T] (serializedObject: Option[(Array[T], Int, Int)] = None) {
case class WrappedArrayBuffer[T] (initialArray : Array[T], initialCapacity : Int, initialSize : Int) {
  
  def this() = this((new Array[AnyRef](16)).asInstanceOf[Array[T]], 16, 0)
  
  // Overloaded constructor Constructor
  def this(initialCapacity : Int) = this((new Array[AnyRef](initialCapacity)).asInstanceOf[Array[T]], initialCapacity, 0)
  
  /**
   * The maximum capacity of the array. 
   * This is the memory allocated to the underlying array
   */
  var capacity = initialCapacity
  
  /**
   *  Mutable state with the number of elements being used in the array
   *  When the currentSize reaches capacity, the capacity will be doubled.
   */
  var currentSize = initialSize
  
  /**
   *  Underlying mutable array 
   */
  private var array = initialArray
    
  /**
   * Returns the underlying mutable array. 
   * Calling code should not modify the array directly.
   */
  def underlying = array
  def underlyingAny = array.map { x => x.asInstanceOf[Any] }
  
  /** The element at given index */
  def apply(index: Int): T = array(index)
  
  /** Expands capacity by doubling the size.
   *  Helps maintain amortized constant insert time
   */
  private def expandCapacity {
    var newArray = (new Array[AnyRef](2 * capacity)).asInstanceOf[Array[T]]
    Array.copy(array, 0, newArray, 0, capacity)
    array = newArray
    capacity = 2*capacity
  }

  /** Set/Update element at given index */
  def update(index: Int, elem: T): Unit = {
    require(index <= currentSize, "Out of bounds access of array buffer")
    if(index == capacity) { 
      // Expand automatically if capacity is reached
      expandCapacity
    }
    array(index) = elem
    if(index == currentSize)
      currentSize = currentSize + 1
  }
  
  /** Inserts an element at a given index, if necessary shifting elements to the right */
  def insertAt(index: Int, elem: T): Unit = {
    require(index <= currentSize, "Out of bounds access of array buffer")
    if(index == capacity || currentSize == capacity) {
      // Expand automatically if capacity is reached
      expandCapacity
    }
    //(index+1 to currentSize).reverse.foreach { x => array(x) = array(x-1) }
    var x=currentSize
    while (x >= index + 1) {
      array(x) = array(x-1)
      x = x - 1
    }
    array(index) = elem
    currentSize = currentSize + 1
  }
  
  /**
   * Merge elements at index and index+1 using a merge function
   */
  def merge(index: Int, mergeFunction : (T,T) => T ): Unit = {
    require(index < currentSize - 1,"Cannot merge the last element of the array with anything")
    array(index) = mergeFunction(array(index), array(index+1))
    //(index+1 until currentSize - 1).foreach { x => array(x) = array(x+1) }
    var x=index + 1
    while (x < currentSize -1){
      array(x) = array(x+1)
      x = x + 1
    }
    currentSize = currentSize - 1
  }
  
  /**
   * Merge elements at index and index+1 replacing the value at "index" using the passed-in value
   */
  def merge(index: Int, value : T ): Unit = {
    require(index < currentSize - 1,"Cannot merge the last element of the array with anything")
    array(index) = value
    // (index+1 until currentSize - 1).foreach { x => array(x) = array(x+1) }
    // Changing to imperative style for better performance (the previous range creates an immutable range object)
    // Another (untested) alternative is
    // Stream.range(index+1, currentSize - 1).foreach { x => array(x) = array(x+1) }
    var x=index + 1
    while (x < currentSize -1){
      array(x) = array(x+1)
      x = x + 1
    }
    currentSize = currentSize - 1
  }
  
  def size = currentSize
  
}