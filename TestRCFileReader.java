package com.letv.map;

import org.apache.commons.lang.CharEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.Properties;

/**
 * Created by wanglifeng1 on 2016/11/24.
 */
public class TestRCFileReader extends Mapper<LongWritable, BytesRefArrayWritable, NullWritable, Text> {

    private ObjectInspector out_oi;
    private LazyBinaryColumnarSerDe serde;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        /*TypeInfo typeInfo = TypeInfoUtils
                .getTypeInfoFromTypeString("struct<a:int,b:string>");
        ObjectInspector oip = TypeInfoUtils
                .getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
        LazyBinaryColumnarSerDe serDe = new LazyBinaryColumnarSerDe();*/
        try {
            StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
                    .getReflectionObjectInspector(OuterStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            String cols = ObjectInspectorUtils.getFieldNames(oi);
            Properties props = new Properties();
            props.setProperty(serdeConstants.LIST_COLUMNS, cols);
            props.setProperty(serdeConstants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
            serde = new LazyBinaryColumnarSerDe();
            SerDeUtils.initializeSerDe(serde, context.getConfiguration(), props, null);
            out_oi = serde.getObjectInspector();
        } catch (SerDeException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
        Object out_o = null;
        try {
            out_o = serde.deserialize(value);
            String out = SerDeUtils.getJSONString(out_o, out_oi);
            context.write(NullWritable.get(), new Text(out));
        } catch (SerDeException e) {
            System.out.println("error deserialize : " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(dfsArgs.length < 2){
            System.out.println("必须参数：input path and output path");
        }

        /*
            hadoop config
         */
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.setBoolean("mapreduce.job.user.classpath.first",true);//解决包冲突，由于hadoop提供的包版本低

        /*
            初始化job
         */
        Job job = Job.getInstance(conf, "testrcreader");
        job.setJarByClass(TestRCFileReader.class);
        job.setMapperClass(TestRCFileReader.class);
        job.setMapOutputValueClass(BytesRefArrayWritable.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(RCFileMapReduceInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        RCFileMapReduceInputFormat.addInputPath(job, new Path(dfsArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(dfsArgs[1]));
        TextOutputFormat.setCompressOutput(job, false);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


    public static class InnerStruct {
        public InnerStruct(String item_id, String settle_type) {
            this.item_id = item_id;
            this.settle_type = settle_type;
        }
        String item_id;
        String settle_type;
    }

    public static class OuterStruct {
        String uuid;
        List<InnerStruct> itemid_infos;
        String cuid;
        Integer t;

        public OuterStruct(String uuid,List<InnerStruct> itemid_infos, String cuid, Integer t) {
            this.uuid = uuid;
            this.itemid_infos = itemid_infos;
            this.cuid = cuid;
            this.t = t;
        }
    }
}
