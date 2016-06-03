package com.thu;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by jagesh on 05/14/2016.
 */
public class MyObject implements Serializable
{
    private static final int COLUMNS = 41;

    private int clusterID;

    private float[] data = new float[COLUMNS];
    public static Map<String,Float> map = new HashMap<>();
    public static List<Float> list = new ArrayList<>();

    public static Map<String,Float> mapFirstCol = new HashMap<String, Float>();
    public static Map<String,Float> mapSecondCol = new HashMap<String, Float>();
    public static Map<String,Float> mapThirdCol = new HashMap<String, Float>();
    static
    {
        mapFirstCol.put("icmp", Float.valueOf(1));
        mapFirstCol.put("tcp", Float.valueOf(2));
        mapFirstCol.put("udp", Float.valueOf(3));
        mapSecondCol.put("aol", Float.valueOf(1));
        mapSecondCol.put("auth", Float.valueOf(2));
        mapSecondCol.put("bgp", Float.valueOf(3));
        mapSecondCol.put("courier", Float.valueOf(4));
        mapSecondCol.put("csnet_ns", Float.valueOf(5));
        mapSecondCol.put("ctf", Float.valueOf(6));
        mapSecondCol.put("daytime", Float.valueOf(7));
        mapSecondCol.put("discard",Float.valueOf(8));
        mapSecondCol.put("domain",Float.valueOf(9));
        mapSecondCol.put("domain_u",Float.valueOf(10));
        mapSecondCol.put("echo",Float.valueOf(11));
        mapSecondCol.put("eco_i",Float.valueOf(12));
        mapSecondCol.put("ecr_i",Float.valueOf(13));
        mapSecondCol.put("efs",Float.valueOf(14));
        mapSecondCol.put("exec",Float.valueOf(15));
        mapSecondCol.put("finger",Float.valueOf(16));
        mapSecondCol.put("ftp",Float.valueOf(17));
        mapSecondCol.put("ftp_data",Float.valueOf(18));
        mapSecondCol.put("gopher",Float.valueOf(19));
        mapSecondCol.put("harvest",Float.valueOf(20));
        mapSecondCol.put("hostnames",Float.valueOf(21));
        mapSecondCol.put("http",Float.valueOf(22));
        mapSecondCol.put("http_2784",Float.valueOf(23));
        mapSecondCol.put("http_443",Float.valueOf(24));
        mapSecondCol.put("http_8001",Float.valueOf(25));
        mapSecondCol.put("imap4",Float.valueOf(26));
        mapSecondCol.put("IRC",Float.valueOf(27));
        mapSecondCol.put("iso_tsap",Float.valueOf(28));
        mapSecondCol.put("klogin",Float.valueOf(29));
        mapSecondCol.put("kshell",Float.valueOf(30));
        mapSecondCol.put("ldap",Float.valueOf(31));
        mapSecondCol.put("link",Float.valueOf(32));
        mapSecondCol.put("login",Float.valueOf(33));
        mapSecondCol.put("mtp",Float.valueOf(34));
        mapSecondCol.put("name",Float.valueOf(35));
        mapSecondCol.put("netbios_dgm",Float.valueOf(36));
        mapSecondCol.put("netbios_ns",Float.valueOf(37));
        mapSecondCol.put("netbios_ssn",Float.valueOf(38));
        mapSecondCol.put("netstat",Float.valueOf(39));
        mapSecondCol.put("nnsp",Float.valueOf(40));
        mapSecondCol.put("nntp",Float.valueOf(41));
        mapSecondCol.put("ntp_u",Float.valueOf(42));
        mapSecondCol.put("other",Float.valueOf(43));
        mapSecondCol.put("pm_dump",Float.valueOf(44));
        mapSecondCol.put("pop_2",Float.valueOf(45));
        mapSecondCol.put("pop_3",Float.valueOf(46));
        mapSecondCol.put("printer",Float.valueOf(47));
        mapSecondCol.put("private",Float.valueOf(48));
        mapSecondCol.put("remote_job",Float.valueOf(49));
        mapSecondCol.put("rje",Float.valueOf(50));
        mapSecondCol.put("shell",Float.valueOf(51));
        mapSecondCol.put("smtp",Float.valueOf(52));
        mapSecondCol.put("sql_net",Float.valueOf(53));
        mapSecondCol.put("ssh",Float.valueOf(54));
        mapSecondCol.put("sunrpc",Float.valueOf(55));
        mapSecondCol.put("supdup",Float.valueOf(56));
        mapSecondCol.put("systat",Float.valueOf(57));
        mapSecondCol.put("telnet",Float.valueOf(58));
        mapSecondCol.put("tftp_u",Float.valueOf(59));
        mapSecondCol.put("tim_i",Float.valueOf(60));
        mapSecondCol.put("time",Float.valueOf(61));
        mapSecondCol.put("urp_i",Float.valueOf(62));
        mapSecondCol.put("uucp",Float.valueOf(63));
        mapSecondCol.put("uucp_path",Float.valueOf(64));
        mapSecondCol.put("vmnet",Float.valueOf(65));
        mapSecondCol.put("whois",Float.valueOf(66));
        mapSecondCol.put("X11",Float.valueOf(67));
        mapSecondCol.put("Z39_50",Float.valueOf(68));
        mapSecondCol.put("urh_i",Float.valueOf(69));
        mapSecondCol.put("icmp",Float.valueOf(70));
        mapThirdCol.put("OTH",Float.valueOf(1));
        mapThirdCol.put("REJ",Float.valueOf(2));
        mapThirdCol.put("RSTO",Float.valueOf(3));
        mapThirdCol.put("RSTOS0",Float.valueOf(4));
        mapThirdCol.put("RSTR",Float.valueOf(5));
        mapThirdCol.put("S0",Float.valueOf(6));
        mapThirdCol.put("S1",Float.valueOf(7));
        mapThirdCol.put("S2",Float.valueOf(8));
        mapThirdCol.put("S3",Float.valueOf(9));
        mapThirdCol.put("SF",Float.valueOf(10));
        mapThirdCol.put("SH",Float.valueOf(11));
    }

    /*
    reading a lines from the file, converting each seperated string by "," to attribute of object and to floating value,
    Then Perform normalization
     */
    public MyObject(String line)
    {
        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        float sum = 0;
        float magnitude = 0;
        for (int col = 0; col < COLUMNS; ++col)
        {
            String token = tokenizer.nextToken();
            if (col == 1)
            {
                data[col] = mapFirstCol.get(token);
            }
            else if (col == 2)
            {
                data[col] = mapSecondCol.get(token);
            }
            else if (col == 3)
            {
                data[col] = mapThirdCol.get(token);
            }
            else
                data[col] = Float.valueOf(token);

            sum += Math.pow(data[col],2);
        }
        magnitude = (float) Math.sqrt(sum);

        for (int i=0;i<data.length; i++)
        {
            data[i] = data[i]/magnitude;
        }
    }

    /*
    segregating the data points to the each cluster based on the Euclidian distance
     */
    public MyObject(MyObject point, List<MyObject> centroids)
    {
        System.arraycopy(point.data, 0 , data, 0, data.length);

        if (centroids.size() == 2)
        {
            float distance1 = Distance(point,centroids.get(0));
            float distance2 = Distance(point,centroids.get(1));
            if (distance1 < distance2)
                clusterID = 0;
            else
                clusterID = 1;
        }

        else if (centroids.size() == 5)
        {
            float d1 = Distance(point,centroids.get(0));
            float d2 = Distance(point,centroids.get(1));
            float d3 = Distance(point,centroids.get(2));
            float d4 = Distance(point,centroids.get(3));
            float d5 = Distance(point,centroids.get(4));
            float leastLength = calculateLeastDistance(d1,d2,d3,d4,d5);
            if (leastLength ==  d1)
                clusterID = 0;
            else if (leastLength ==  d2)
                clusterID = 1;
            else if (leastLength ==  d3)
                clusterID = 2;
            else if (leastLength ==  d4)
                clusterID = 3;
            else if (leastLength ==  d5)
                clusterID = 4;
        }
    }

    /*
    if cluster size (K=5) is five, finding a least distance
     */
    private float calculateLeastDistance(float d1, float d2, float d3, float d4,float d5)
    {
        float lDist = 0;
        if (d1 <d2 && d1<d3 && d1<d4 && d1<d5 )
            lDist = d1;
        if (d2 <d1 && d2<d3 && d2<d4 && d2<d5 )
            lDist = d2;
        if (d3 <d1 && d3<d2 && d3<d4 && d3<d5 )
            lDist = d3;
        if (d4 <d1 && d4<d3 && d4<d2 && d4<d5 )
            lDist = d4;
        if (d5 <d1 && d5<d3 && d5<d2 && d5<d2 )
            lDist = d5;

        return lDist;
    }

    /*
    for reduce function in Spark, adding two data of type object MyObject
     */
    public MyObject(MyObject a, MyObject b)
    {
        for (int i=0;i<data.length; i++)
        {
            data[i] = a.data[i] + b.data[i];
        }
    }

    /*
    Copying the centroid and clusterID as a value
     */
    public MyObject(MyObject centroid)
    {
        System.arraycopy(centroid.data, 0, data, 0, data.length);
        clusterID = centroid.clusterID;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyObject myObject = (MyObject) o;
        return clusterID == myObject.clusterID &&
                Arrays.equals(data, myObject.data);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(clusterID, data);
    }

    @Override
    public String toString()
    {
        return "MyObject{" +
                "clusterID=" + clusterID +
                ", data=" + Arrays.toString(data) +
                '}';
    }

    public float Distance(MyObject point, MyObject myObject)
    {
        float sum =0;
        for (int i=0;i<point.data.length; i++)
        {
            sum += (Math.pow(point.data[i] - myObject.data[i] ,2.0));
        }
        return (float) Math.sqrt(sum);
    }

    public int getClusterID()
    {
        return clusterID;
    }

    public void setClusterID(int clusterID)
    {
        this.clusterID = clusterID;
    }

    public void changePosition(MyObject reduce, long count)
    {
        for (int i=0;i<data.length;i++)
        {
            BigDecimal bd = new BigDecimal(reduce.data[i]/count);
            bd = bd.setScale(7,BigDecimal.ROUND_HALF_DOWN);
            data[i] = bd.floatValue();
        }
    }
}
