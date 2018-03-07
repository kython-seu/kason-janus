package java8.study;

public class TestID {

    public static void main(String[] args) {
        //count = 15143772206410940  (一開始隨機生成的countId)

        //long count = 15143772206410940l;
        //System.out.println( (count << 6) | 5); //969201421210300165 vertexId = count << 6 | 5


        //(vertexId >>> 6) << 1 + dir雞typeID
        //System.out.println(969201421210300165l >>> 6); //15143772206410940
        //System.out.println(15143772206410940l << 1); //30287544412821880


        //計算 numVariableBlocks(unsignedBitLength(value)+prefixBitLen);  numVariableBlocks就是把數據分成幾塊
        /**
         * private static int numVariableBlocks(final int numBits) {
         assert numBits > 0;
         return (numBits - 1) / 7 + 1;
         }
         */
        //unsignedBitLength(value) = Long.SIZE - Long.numberOfLeadingZeros(value);
        //System.out.println(64 - Long.numberOfLeadingZeros(30287544412821880l)); //55 + prefixBitLen = 58  (58 - 1) / 7 + 1 = 8 + 1 = 9個block
        //說明需要9個block來存儲typeID 30287544412821880
        //所以後面調用WriteBuffer b = new WriteByteBuffer(relationTypeLength(relationTypeId));的時候就是申請9個Byte字節空間的內存。 內部維護的是ByteBuffer


        //測試邊， 出邊
        long count = 16544096211840532l; //count 原始值

        long vertexId = (count << 6) | 21;
        System.out.println(vertexId); //vertexId = 1058822157557794069

        //計算IDManager.stripEntireRelationTypePadding(relationTypeId) << 1 去除padding 在<< 1
        System.out.println(vertexId >>> 6);//16544096211840532
        System.out.println(16544096211840532l << 1);//33088192423681064

        //計算 numVariableBlocks(unsignedBitLength(value)+prefixBitLen);
        //先計算unsignedBitLength(value)
        System.out.println(64 - Long.numberOfLeadingZeros(33088192423681064l)); //55 + 3 = 58  （58 -1） /7 + 1 = 8 + 1 = 9 block
        //說明需要9個block來存儲typeID 33088192423681064
        //所以後面調用WriteBuffer b = new WriteByteBuffer(relationTypeLength(relationTypeId));的時候就是申請9個Byte字節空間的內存。 內部維護的是ByteBuffer

        //後面執行IDHandler.writeRelationType(b, relationTypeId, dirID, invisible); //relationTypeId 1058822157557794069 dirID EDGE_OUT insible false
        //1， 計算typeId , 2, writePositiveWithPrefix， 相當於把vertex以及prefix全部寫進去
        //2 轉到writePositiveWithPrefix(final WriteBuffer out, long value, long prefix, final int prefixBitLen)， value就是typeId
        //prefix就是前綴， prefixBitLen是前綴的位數， 比如此處typeId 是33088192423681064， prefix是（EDGE_OUT) 011,


    }
}
