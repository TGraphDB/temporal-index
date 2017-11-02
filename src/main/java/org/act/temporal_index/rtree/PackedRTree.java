package org.act.temporal_index.rtree;

import java.util.ArrayList;
import java.util.List;

abstract public class PackedRTree<C extends Coordinate<? super C>> extends RTree<C>
{
    private static int b = 4096/32;
    protected final CoordinateGen<C> coordinateGen;

    public PackedRTree(CoordinateGen<C> coordinateGen){
        this.coordinateGen = coordinateGen;
    }

    abstract protected void sort(List<RNode<C>> data);

    protected RNode<C> pack(List<RNode<C>> data)
    {
        this.sort(data);
        List<RNode<C>> upperLevelNodes = new ArrayList<>();
        for(int i=b; i<=data.size(); i+=b)
        {
            RNode<C> node = RNode.newInstance(coordinateGen);
            node.setChildren(data.subList(i-b, i));
            node.updateBound();
            upperLevelNodes.add(node);
        }
        if(data.size()%b!=0){
            int s = (data.size()/b)*b;
            RNode<C> node = RNode.newInstance(coordinateGen);
            node.setChildren(data.subList(s, data.size()));
            node.updateBound();
            upperLevelNodes.add(node);
        }
        RTree.log.info("one level packed, ({}) nodes", upperLevelNodes.size());
        if(upperLevelNodes.size()>1){
            return pack(upperLevelNodes);
        }else if(upperLevelNodes.size()==1){
            return upperLevelNodes.get(0);
        }else{
            throw new RuntimeException("should not happen");
        }
    }

    public static class HilbertRTree<C extends Coordinate<? super C>> extends PackedRTree<C>
    {
        public HilbertRTree(List<RNode<C>> list, CoordinateGen<C> coordinateGen) {
            super(coordinateGen);
            this.setRoot(this.pack(list));
        }

        //+++++++++++++++++++++++++++ PUBLIC-DOMAIN SOFTWARE ++++++++++++++++++++++++++
        // Functions: TransposetoAxes  AxestoTranspose
        // Purpose:   Transform in-place between Hilbert transpose and geometrical axes
        // Example:   b=5 bits for each of n=3 coordinates.
        //            15-bit Hilbert integer = A B C D E F G H I J K L M N O  is stored
        //            as its Transpose
        //                   X[0] = A D G J M                X[2]|
        //                   X[1] = B E H K N    <------->       | /X[1]
        //                   X[2] = C F I L O               axes |/
        //                          high  low                    0------ X[0]
        //            Axes are stored conventially as b-bit integers.
        // Author:    John Skilling  20 Apr 2001 to 11 Oct 2003
        //-----------------------------------------------------------------------------

//        //t; // char,short,int for up to 8,16,32 bits per word
//        void TransposetoAxes( int* X, int b, int n )  // position, #bits, dimension
//        {    int
//            t  N = 2 << (b-1), P, Q, t;
//            int  i;
//// Gray decode by H ^ (H/2)
//            t = X[n-1] >> 1;
//            for( i = n-1; i >= 0; i-- ) X[i] ^= X[i-1];
//            X[0] ^= t;
//// Undo excess work
//            for( Q = 2; Q != N; Q <<= 1 ) {
//                P=Q-1;
//                for( i = n-1; i >= 0 ; i-- )
//                    if( X[i] & Q ) X[0] ^= P;                              // invert
//                    else{ t = (X[0]^X[i]) & P; X[0] ^= t; X[i] ^= t; }  }  // exchange
//        }
//        void AxestoTranspose( int
//                                      t* X, int b, int n )  // position, #bits, dimension
//        {    int
//            t  M = 1 << (b-1), P, Q, t;
//            int  i;
//// Inverse undo
//            for( Q = M; Q > 1; Q >>= 1 ) {
//                P=Q-1;
//                for( i = 0; i < n; i++ )
//                    if( X[i] & Q ) X[0] ^= P;                              // invert
//                    else{ t = (X[0]^X[i]) & P; X[0] ^= t; X[i] ^= t; }  }  // exchange
//// Gray encode
//            for( i = 1; i < n; i++ ) X[i] ^= X[i-1];
//            t=0;
//            for( Q = M; Q > 1; Q >>= 1 )
//                if( X[n-1] & Q ) t ^= Q-1;
//            for( i = 0; i < n; i++ ) X[i] ^= t;
//        }
//        main()
//        {    int
//            t X[3] = {5,10,20};  // any position in 32x32x32 cube
//            AxestoTranspose(X, 5, 3);  // Hilbert transpose for 5 bits and 3 dimensions
//            printf("Hilbert integer = %d%d%d%d%d%d%d%d%d%d%d%d%d%d%d = 7865 check\n",
//                    X[0]>>4 & 1, X[1]>>4 & 1, X[2]>>4 & 1, X[0]>>3 & 1, X[1]>>3 & 1,
//                    X[2]>>3 & 1, X[0]>>2 & 1, X[1]>>2 & 1, X[2]>>2 & 1, X[0]>>1 & 1,
//                    X[1]>>1 & 1, X[2]>>1 & 1, X[0]>>0 & 1, X[1]>>0 & 1, X[2]>>0 & 1);
//        }

        @Override
        protected void sort(List<RNode<C>> data) {

        }
    }

    public static class STRTree<C extends Coordinate<? super C>> extends PackedRTree<C>
    {

        public STRTree(List<RNode<C>> list, CoordinateGen<C> coordinateGen) {
            super(coordinateGen);
            this.setRoot(this.pack(list));
        }

        @Override
        protected void sort(List<RNode<C>> data) {
            int k = this.coordinateGen.newInstance().dimensionCount();
            recursiveSort(data, 0, data.size(), 0, k);
        }

        private void recursiveSort(List<RNode<C>> data, int left, int right, int corIndex, int k) {
            data.subList(left, right).sort(RNode.getComparator(this.coordinateGen.newInstance(), corIndex));
            if(k>1) {
                int r = right - left;
                int p = r / b + (r % b == 0 ? 0 : 1);
                int s = (int) Math.round(Math.ceil(Math.pow(p, 1d / k)));
                int groupLen = s * b;
                for (int i = 0; i < s; i++) {
                    int start = i * groupLen;
                    int end = i * (groupLen + 1) > right ? right : i * (groupLen + 1);
                    recursiveSort(data, start, end, corIndex + 1, k - 1);
                }
            }
        }


    }


}
