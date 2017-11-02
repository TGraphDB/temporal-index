package org.act.temporal_index.rtree;

import java.util.Comparator;

public abstract class Coordinate<COR extends Coordinate<? super COR>>
{
    abstract public Comparator<? super COR> getComparator(int dimIndex);

    abstract public int dimensionCount();

    abstract public void setToAvg(COR minBound, COR maxBound);

    abstract public void updateMin(COR value);

    abstract public void updateMax(COR value);

    public abstract boolean lessEqualThan(COR cor, int dimIndex);

    public static abstract class Tuple4<
            A extends Comparable<A>,
            B extends Comparable<B>,
            C extends Comparable<C>,
            D extends Comparable<D>>
            extends Coordinate<Tuple4<A,B,C,D>>{
        private A a;
        private B b;
        private C c;
        private D d;

        public A getA() {
            return a;
        }

        public void setA(A a) {
            this.a = a;
        }

        public B getB() {
            return b;
        }

        public void setB(B b) {
            this.b = b;
        }

        public C getC() {
            return c;
        }

        public void setC(C c) {
            this.c = c;
        }

        public D getD() {
            return d;
        }

        public void setD(D d) {
            this.d = d;
        }

        @Override
        public Comparator<Tuple4<A,B,C,D>> getComparator(int dimIndex){
            switch (dimIndex){
                case 0: return Comparator.comparing(Tuple4::getA);
                case 1: return Comparator.comparing(Tuple4::getB);
                case 2: return Comparator.comparing(Tuple4::getC);
                default:return Comparator.comparing(Tuple4::getD);
            }
        }

        @Override
        public int dimensionCount() {
            return 4;
        }

        @Override
        public void updateMin(Tuple4<A,B,C,D> value) {
            if(this.a.compareTo(value.getA())>0){
                this.a = value.getA();
            }
            if(this.b.compareTo(value.getB())>0){
                this.b = value.getB();
            }
            if(this.c.compareTo(value.getC())>0){
                this.c = value.getC();
            }
            if(this.d.compareTo(value.getD())>0){
                this.d = value.getD();
            }
        }

        @Override
        public void updateMax(Tuple4<A,B,C,D> value) {
            if(this.a.compareTo(value.getA())<0){
                this.a = value.getA();
            }
            if(this.b.compareTo(value.getB())<0){
                this.b = value.getB();
            }
            if(this.c.compareTo(value.getC())<0){
                this.c = value.getC();
            }
            if(this.d.compareTo(value.getD())<0){
                this.d = value.getD();
            }
        }

        @Override
        public boolean lessEqualThan(Tuple4<A,B,C,D> t, int dimIndex) {
            switch (dimIndex){
                case 0: return this.a.compareTo(t.getA())<=0;
                case 1: return this.b.compareTo(t.getB())<=0;
                case 2: return this.c.compareTo(t.getC())<=0;
                default:return this.d.compareTo(t.getD())<=0;
            }
        }


    }
}
