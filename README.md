# 911-call-data-analytics
Data analysis of 911 emergency call using Hadoop MapReduce

#### 1. start
입력 분석 데이터 (911.csv) 업로드
```
./bin/hadoop fs -put ~/proj/911.csv 911.csv
```
</br>

#### 2. execute
  * **모든 신고 발생 비율 출력** </br>
  사용자 입력: 없음
  ```
  ~/hadoop/bin/hadoop jar call911_result.jar mju.hadoop.call911.Call911 911.csv call911_result_1
  ```
  <img width="500" alt="result1" src="https://user-images.githubusercontent.com/33407123/80941151-9ba91780-8e1c-11ea-8eb4-5bc56fe0dc42.png">
</br>

  * **입력한 신고 유형 발생 비율 출력** </br>
  사용자 입력: EMS | Traffinc | Fire
  ```
  ~/hadoop/bin/hadoop jar call911_result.jar mju.hadoop.call911.Call911 911.csv call911_result_2_fire Fire
  ```
  <img width="500" alt="result1" src="https://user-images.githubusercontent.com/33407123/80941297-183bf600-8e1d-11ea-9c58-06f5ebe1d357.png">
</br></br>
