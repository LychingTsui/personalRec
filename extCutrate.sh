
for i in `seq 1 31`; do
    day=`date -d "$i days ago " "+%Y%m%d"`
    dayArr[$i-1]=$day
done

jarName=Features.jar
movieItemFeatPath=$movieItemFeatures   # 调用俄ext1.sh export 的变量

jarPath=/home/tvapk/run_sh/cuiliqing/
showdataPath=/data/tvapk/show/
hdfsPath=/user/tvapk/cuiliqing/
hadoop=/opt/hadoop/hadoop-2.6.0-cdh5.7.1/bin/hadoop
vvdataPath=/user/tvapk/peichao/TV/movieper/20160420/vvlog/
dataSavePath=/data/tvapk/cuiliqing/cuiliqing/
getvvsh=getvvhistory.sh
dateStart=201711 # 设置收集vv历史正例起始日期
export dateStart
sh $jarPath$getvvsh
out=$dateStart"2TodayvvHistory"

rm -rf ${dataSavePath}dataSet_cutrate        #保训练集测试集预留本的本地路径
mkdir ${dataSavePath}dataSet_cutrate
#dayStart=${arr[0]}
#ymd=`date +%Y%m%d`
#year=${ymd:0:4}
rm $dataSavePath"totvvguid.txt"
extdir="extWithvvHistory"
$hadoop fs -rm -r $hdfsPath$extdir
$hadoop fs -mkdir $hdfsPath$extdir

$hadoop fs -rm -r ${hdfsPath}showdata
$hadoop fs -mkdir ${hdfsPath}showdata

for s in ${dayArr[@]}
do
  
    $hadoop fs -rm -r $hdfsPath$s"vvtmp01"
    $hadoop jar $jarPath$jarName PersonalRecommend.GetDayVvGuid $vvdataPath$s $hdfsPath$s"vvtmp01"
    $hadoop fs -rm -r $hdfsPath$s"vvguid"
    $hadoop fs -cp $hdfsPath$s"vvtmp01"/part-r-00000 $hdfsPath$s"vvguid"
    rm $dataSavePath$s"vvguid"
    $hadoop fs -get $hdfsPath$s"vvguid" $dataSavePath
    $hadoop fs -rm -r $hdfsPath$s"vvguid"
    $hadoop fs -rm -r $hdfsPath$s"vvtmp01"
    cat $dataSavePath$s"vvguid" >> $dataSavePath"totvvguid.txt"
    rm $dataSavePath$s"vvguid" 
    monday=${s:4}
   # $hadoop fs -rm -r $hdfsPath$monday".txt"
    $hadoop fs -put $showdataPath$monday".txt" ${hdfsPath}showdata/ 
    
done
    $hadoop fs -rm -r ${hdfsPath}negandhistory
    $hadoop fs -mkdir ${hdfsPath}negandhistory

    $hadoop fs -rm -r ${hdfsPath}showMvidtmp 
    $hadoop jar $jarPath$jarName com.qiguo.tv.movie.model.CollectPersonalShow ${hdfsPath}showdata/ ${hdfsPath}showMvidtmp 
    $hadoop fs -mv ${hdfsPath}showMvidtmp/part-r-00000 ${hdfsPath}negandhistory/
    $hadoop fs -rm -r  ${hdfsPath}showMvidtmp 
    $hadoop fs -rm -r ${hdfsPath}showdata

    $hadoop fs -rm -r $hdfsPath$extdir/neg
    $hadoop fs -rm -r ${hdfsPath}negtmp
    $hadoop fs -cp $hdfsPath$out ${hdfsPath}negandhistory/
    $hadoop jar $jarPath$jarName com.qiguo.tv.movie.model.ExtNegsfromPersonalShow ${hdfsPath}negandhistory/  ${hdfsPath}negtmp  
    
    $hadoop fs -cp ${hdfsPath}negtmp/part-r-00000 $hdfsPath$extdir/neg 
    $hadoop fs -rm -r ${hdfsPath}negtmp
    $hadoop fs -rm -r ${hdfsPath}negandhistory
    
    $hadoop fs -rm -r $hdfsPath"totvvguid.txt" 
    $hadoop fs -put $dataSavePath"totvvguid.txt" $hdfsPath 
    
    $hadoop fs -rm -r $hdfsPath"postmp"
    $hadoop jar $jarPath$jarName com.qiguo.tv.movie.model.GetPosMovIdLabel01 $hdfsPath$out  $hdfsPath"postmp" $hdfsPath"totvvguid.txt"
    $hadoop fs -mv $hdfsPath"postmp"/part-r-00000  $hdfsPath$extdir/
    $hadoop fs -rm -r $hdfsPath"postmp"   
    $hadoop fs -rm -r $hdfsPath"totvvguid.txt" 
    rm $dataSavePath"totvvguid.txt" 

    input=$hdfsPath$extdir
    output=/user/tvapk/cuiliqing/outtmp  # 输出路径
    
    $hadoop fs -rm -r $output
    $hadoop fs -rm -r  ${hdfsPath}tmpfile
    $hadoop fs -mkdir  ${hdfsPath}tmpfile
    #  第二个MapReduce 把对应电影ID 换成 其对应的特征值 格式 guid 0/1 [featureId:val...]
    $hadoop jar ${jarPath}${jarName}  com.qiguo.tv.movie.featCollection.GetCombineGuid_Item_label01 $hdfsPath$extdir  $output $movieItemFeatPath 
    #$hadoop fs -mv ${hdfsPath}$tmpFile01 ${hdfsPath}"tmpfile"/  #中间结果保留路径 
    $hadoop fs -rm -r  ${hdfsPath}join
    $hadoop fs -mkdir ${hdfsPath}join 
    #$hadoop fs -rm -r ${hdfsPath}join/part-r-00000
    $hadoop fs -mv ${hdfsPath}personalLikes ${hdfsPath}join/
    #$hadoop fs -rm -r ${hdfsPath}join/part-r-00001 #####某天的用户行为数据  part-r-00000 是用户偏好特征注意更新
    $hadoop fs -mv ${output}/part-r-00000 ${hdfsPath}join/
    $hadoop fs -rm -r $output
    #第三个MapReduce   输出标准的Liblinear 格式数据待按比例分割训练测试集后 可参与训练测试
    $hadoop jar ${jarPath}${jarName} com.qiguo.tv.movie.featCollection.CombineMovieFeatsandLikesFeatures ${hdfsPath}join $output $mvTagsStartId $likesTagsStartId $personalLikesIdStart
    $hadoop fs -rm -r ${hdfsPath}dataset
    $hadoop fs -cp $output/part-r-00000 ${hdfsPath}dataset
    $hadoop fs -rm -r $output 
    rm ${dataSavePath}dataset
    $hadoop fs -get ${hdfsPath}dataset $dataSavePath
    #$hadoop fs -rm -r $output
    $hadoop fs -rm -r ${hdfsPath}dataset

    lines=`cat ${dataSavePath}dataset | wc -l`
    posTotal=`cat ${dataSavePath}dataset | awk '$1==1.0' | wc -l`      #统计正例样本个数
    echo "正例样本总数： $posTotal"
    negTotal=`cat ${dataSavePath}dataset | awk '$1==0.0' | wc -l`      # 统计负例样本个数
    echo "负例样本总数： $negTotal"

    posLineTest=$[posTotal/5]
    echo "正例测试样本数目： $posLineTest"
    posLineTrain=$[posTotal-posLineTest]
    echo "正例训练样本数目： $posLineTrain"
    negLineTest=$[negTotal/5]
    echo "负例测试样本数目； $negLineTest"
    negLineTrain=$[negTotal-negLineTest]
    echo "负例训练样本数目: $negLineTrain"

    # 按比例分割训练集 测绘集
    cat ${dataSavePath}dataset | awk '$1==1.0' | head  -$posLineTrain >> ${dataSavePath}dataSet_cutrate/train.txt
    cat ${dataSavePath}dataset | awk '$1==1.0' | tail  -$posLineTest >> ${dataSavePath}dataSet_cutrate/test.txt
    cat ${dataSavePath}dataset | awk '$1==0.0' | head  -$negLineTrain >> ${dataSavePath}dataSet_cutrate/train.txt
    cat ${dataSavePath}dataset | awk '$1==0.0' | tail  -$negLineTest  >> ${dataSavePath}dataSet_cutrate/test.txt
    rm ${dataSavePath}dataset 
    echo "路径下 ${dataSavePath}dataSet_cutrate/train.txt 训练样本数据文件生成"
    echo "路径下 ${dataSavePath}dataSet_cutrate/test.txt 测试样本数据文件生成"



