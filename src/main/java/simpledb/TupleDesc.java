package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * the nums of the Fields in the TupleDesc
     */
    private int numFields;

    /**
     *
     */
    private TDItem[] tdArs;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldType = t;
            this.fieldName = n;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof TDItem) {
                TDItem another = (TDItem) o;
                //因为fieldName可能为null,所以都为null时视为name相同
                boolean nameEquals = (fieldName == null && another.fieldName == null)
                        || fieldName.equals(another.fieldName);
                boolean typeEquals = fieldType.equals(another.fieldType);
                return nameEquals && typeEquals;
            } else {
                return false;
            }
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.numFields = typeAr.length;
        tdArs = new TDItem[numFields];
        for (int i = 0; i < numFields; i++) {
            tdArs[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }

    private TupleDesc(TDItem[] tdItems) {
        if (tdItems == null || tdItems.length == 0) {
            throw new IllegalArgumentException("tdItem数组必须非空且至少包含一个元素");
        }
        this.tdArs = tdItems;
        this.numFields = tdItems.length;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (!isValidIndex(i)) {
            throw new IllegalArgumentException("Field 索引值不合法");
        }
        return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (!isValidIndex(i)) {
            throw new IllegalArgumentException("Field 索引值不合法");
        }
        return tdArs[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {

        for (int i = 0; i < numFields; i++) {
            if(name == null && tdArs[i].fieldName == null){
                return i;
            }
            if (name != null && name.equals(tdArs[i].fieldName)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int res = 0;
        for (int i = 0; i < tdArs.length; i++) {
            res += tdArs[i].fieldType.getLen();
        }
        return res;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TDItem[] tdItems1 = td1.tdArs;
        TDItem[] tdItems2 = td2.tdArs;
        int length1 = tdItems1.length;
        int length2 = tdItems2.length;
        TDItem[] resultItems = new TDItem[length1 + length2];
        System.arraycopy(tdItems1, 0, resultItems, 0, length1);
        System.arraycopy(tdItems2, 0, resultItems, length1, length2);
        return new TupleDesc(resultItems);
    }

    /**
     * check whether the index is in the range or not
     * @param index
     * @return boolean
     */
    private boolean isValidIndex(int index) {
        return index >= 0 && index < tdArs.length;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) return true;

        if (o instanceof TupleDesc){
            TupleDesc other = (TupleDesc) o;
            if (other.numFields != this.numFields){
                return false;
            }
            for (int i = 0; i < numFields; i++){
                if (!this.tdArs[i].equals(other.tdArs[i])){
                    return false;
                }
            }
            return true;
        }else {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }

}
