package org.act.temporal_index.rtree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RTree<C extends Coordinate<? super C>>
{
    public static Logger log = LoggerFactory.getLogger("test");
    public static long nodeAccessCount = 0;

    private RNode<C> root;

    public List<RNode<C>> query(CoordinateRange<C> range){
        return root.query(range);
    }

    public void setRoot(RNode<C> root) {
        this.root = root;
    }

    public static abstract class CoordinateGen<T extends Coordinate<?super T>>{
        abstract public T newInstance(T center);
        abstract public T newInstance();
    }

    // ---------- for test ------------

    public static class GPSData extends Coordinate.Tuple4<Integer, Integer, Double, Double>
    {
        public GPSData(int carNo, int time, double longitude, double latitude) {
            this.setA(carNo);
            this.setB(time);
            this.setC(longitude);
            this.setD(latitude);
        }

        public GPSData() {
            this.setA(0);
            this.setB(0);
            this.setC(0d);
            this.setD(0d);
        }

        public GPSData(GPSData data) {
            this.setA(data.getA());
            this.setB(data.getB());
            this.setC(data.getC());
            this.setD(data.getD());
        }

        @Override
        public void setToAvg(Tuple4<Integer, Integer, Double, Double> minBound, Tuple4<Integer, Integer, Double, Double> maxBound) {
            this.setA((minBound.getA()+maxBound.getA())/2);
            this.setB((minBound.getB()+maxBound.getB())/2);
            this.setC((minBound.getC()+maxBound.getC())/2);
            this.setD((minBound.getD()+maxBound.getD())/2);
        }

        @Override
        public String toString() {
            return "GPSData{"+this.getA()+","+this.getB()+","+this.getC()+","+this.getD()+"}";
        }
    }

    public static class GPSDataGen extends CoordinateGen<GPSData>{
        @Override
        public GPSData newInstance(GPSData center){
            return new GPSData(center);
        }
        @Override
        public GPSData newInstance(){
            return new GPSData();
        }
    }

    public static List<RNode<GPSData>> packGPS2Node(List<GPSData> data){
        List<RNode<GPSData>> result = new ArrayList<>();
        GPSDataGen gen = new GPSDataGen();
        for(GPSData d : data){
            RNode<GPSData> node = new RNode<>(gen, d);
            result.add(node);
        }
        return result;
    }

    public static void main(String[] args) throws IOException
    {
        log.info("program start.");

        BufferedReader ir = new BufferedReader(new FileReader(new File("cache.anonymous.in.txt")),1024*1024*20);
        List<GPSData> list = new ArrayList<>(1007_0000);
        String lineTxt;
        while((lineTxt = ir.readLine()) != null) {
            String[] arr = lineTxt.split("\t");
            int carNo = Integer.parseInt(arr[0]);
            int timeStampSecond = Integer.parseInt(arr[1]);
            double latitude = Double.parseDouble(arr[2]);
            double longitude = Double.parseDouble(arr[3]);
            list.add(new GPSData(carNo, timeStampSecond, longitude, latitude));
        }
        ir.close();

        log.info("data load done");



        List<RNode<GPSData>> data = packGPS2Node(list);

        log.info("data packed.");

        RTree<GPSData> strTree = new PackedRTree.STRTree<>(data, new GPSDataGen());
        RTree<GPSData> hilbertTree = new PackedRTree.HilbertRTree<>(data, new GPSDataGen());

        log.info("rtree built. range {}", strTree.root.getBound());

        CoordinateRange<GPSData> queryRange = new CoordinateRange<GPSData>(
                new GPSData(1, 0, 116.34, 39.8),
                new GPSData(10, 2000, 116.56, 39.9));

        List<RNode<GPSData>> result = strTree.query(queryRange);

        log.info("query done. answer({}) node access count({})", result.size(), RTree.nodeAccessCount);

//        for(RNode<GPSData> node : result)
//        {
//            log.info("{}", node.getCenter());
//        }
        RTree.nodeAccessCount=0;

        result = strTree.query(queryRange);
        log.info("query done. answer({}) node access count({})", result.size(), RTree.nodeAccessCount);
        RTree.nodeAccessCount=0;
//        for(RNode<GPSData> node : data)
//        {
//            if(queryRange.contains(node.getCenter())){
//                log.info("{}", node);
//            }
//        }

    }
}
