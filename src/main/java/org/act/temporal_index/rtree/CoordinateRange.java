package org.act.temporal_index.rtree;

public class CoordinateRange<C extends Coordinate<? super C>>
{
    private C min;
    private C max;

    public CoordinateRange(C min, C max) {
        this.min = min;
        this.max = max;
    }

    public C getMin() {
        return min;
    }

    public void setMin(C min) {
        this.min = min;
    }

    public C getMax() {
        return max;
    }

    public void setMax(C max) {
        this.max = max;
    }

    public boolean overlap(CoordinateRange<C> bound) {

        C min1 = this.min;
        C min2 = bound.min;
        C max1 = this.max;
        C max2 = bound.max;
        for(int i=0;i<min.dimensionCount();i++){
            if(!(min1.lessEqualThan(max2, i) && min2.lessEqualThan(max1, i))){
//                RTree.log.info("FALSE NODE {} BOUND {}", this, bound);
                return false;
            }
        }
//        RTree.log.info("TRUE  NODE {} BOUND {}", this, bound);
        return true;
    }

    public boolean contains(C cor) {
        C min = this.min;
        C max = this.max;
        for(int i = 0; i< this.min.dimensionCount(); i++){
            if(!(min.lessEqualThan(cor, i) && cor.lessEqualThan(max, i))){
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "CoordinateRange{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
