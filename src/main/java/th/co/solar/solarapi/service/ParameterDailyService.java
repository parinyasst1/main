package th.co.solar.solarapi.service;

import com.google.firebase.database.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import th.co.solar.solarapi.model.TotalDaily;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ParameterDailyService {

    // Get a reference to our posts
    FirebaseDatabase database = null;

    DatabaseReference namePath = null;

    DatabaseReference dailyG1 = null;
    DatabaseReference dailyG2 = null;
    DatabaseReference dailyG3 = null;

    DatabaseReference dailyG4 = null;
    DatabaseReference dailyG5 = null;
    DatabaseReference dailyG6 = null;

    Map<String, BigDecimal> gridkwTall = new HashMap<>();
    Map<String, BigDecimal> LoadkwTall = new HashMap<>();

    boolean[] isName = {true};

    boolean[] isStart1G1 = {true};
    boolean[] isStart1G2 = {true};
    boolean[] isStart1G3 = {true};
    boolean[] isStart1G4 = {true};
    boolean[] isStart1G5 = {true};
    boolean[] isStart1G6 = {true};

    List<String> siteList = new ArrayList<>();

    Map<String, Object> dailyTotal = new HashMap<>();

    public Map<String, Object> processQueueTotal() {

        gridkwTall = new HashMap<>();
        LoadkwTall = new HashMap<>();

        boolean[] isName = {true};

        isStart1G1[0] = true;
        isStart1G2[0] = true;
        isStart1G3[0] = true;
        isStart1G4[0] = true;
        isStart1G5[0] = true;
        isStart1G6[0] = true;

        gridkwTall = new HashMap<>();
        LoadkwTall = new HashMap<>();

        database = FirebaseDatabase.getInstance();

        namePath = database.getReference("NamePath");

        dailyG1 = database.getReference("ParameterDailyG1");
        dailyG2 = database.getReference("ParameterDailyG2");
        dailyG3 = database.getReference("ParameterDailyG3");
        dailyG4 = database.getReference("ParameterDailyG4");
        dailyG5 = database.getReference("ParameterDailyG5");
        dailyG6 = database.getReference("ParameterDailyG6");

        dailyTotal = new HashMap<>();

        namePath.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if(isName[0]) {
                    siteList = (List<String>) dataSnapshot.getValue();
                    log.info("siteList : {}", siteList);
                    getDailyG1();
                    isName[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });

        return null;
    }

    ////////////////////////////////////// TODO getDailyG1
    public void getDailyG1(){
        dailyG1.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G1";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        try {
                            getData(obj,site);
                        }catch (Exception ex){
                            ex.printStackTrace();
                        }
                    }
                }
                if(isStart1G1[0]) {
                    log.info("getDailyG1");
                    getDailyG2();
                    isStart1G1[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }
    ////////////////////////////////////// TODO getDailyG2
    public void getDailyG2(){
        dailyG2.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G2";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                if(hashMapData == null){
                    if(isStart1G2[0]) {
                        updateTotal();
                        isStart1G2[0] = false;
                        return;
                    }
                }
                for(String site:siteList){
                    String key = site+group;
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        try {
                            getData(obj,site);
                        }catch (Exception ex){
                            ex.printStackTrace();
                        }
                    }
                }
                if(isStart1G2[0]) {
                    log.info("getDailyG2");
                    getDailyG3();
                    isStart1G2[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }
    ////////////////////////////////////// TODO getDailyG3
    public void getDailyG3(){
        dailyG3.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G3";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                if(hashMapData == null){
                    if(isStart1G3[0]) {
                        updateTotal();
                        isStart1G3[0] = false;
                        return;
                    }
                }
                for(String site:siteList){
                    String key = site+group;
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        try {
                            getData(obj,site);
                        }catch (Exception ex){
                            ex.printStackTrace();
                        }
                    }
                }
                if(isStart1G3[0]) {
                    log.info("getDailyG3");
                    getDailyG4();
                    isStart1G3[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }
    ////////////////////////////////////// TODO getDailyG4
    public void getDailyG4(){
        dailyG4.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G4";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                if(hashMapData == null){
                    if(isStart1G4[0]) {
                        updateTotal();
                        isStart1G4[0] = false;
                        return;
                    }
                }
                for(String site:siteList){
                    String key = site+group;
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        try {
                            getData(obj,site);
                        }catch (Exception ex){
                            ex.printStackTrace();
                        }
                    }
                }
                if(isStart1G4[0]) {
                    log.info("getDailyG4");
                    getDailyG5();
                    isStart1G4[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }
    ////////////////////////////////////// TODO getDailyG5
    public void getDailyG5(){
        dailyG5.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G5";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                if(hashMapData == null){
                    if(isStart1G5[0]) {
                        updateTotal();
                        isStart1G5[0] = false;
                        return;
                    }
                }
                for(String site:siteList){
                    String key = site+group;
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        try {
                            getData(obj,site);
                        }catch (Exception ex){
                            ex.printStackTrace();
                        }
                    }
                }
                if(isStart1G5[0]) {
                    log.info("getDailyG5");
                    getDailyG6();
                    isStart1G5[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }
    ////////////////////////////////////// TODO getDailyG6
    public void getDailyG6(){
        dailyG6.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G6";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                if(hashMapData == null){
                    if(isStart1G6[0]) {
                        updateTotal();
                        isStart1G6[0] = false;
                        return;
                    }
                }
                for(String site:siteList){
                    String key = site+group;
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        try {
                            getData(obj,site);
                        }catch (Exception ex){
                            ex.printStackTrace();
                        }
                    }
                }
                if(isStart1G6[0]) {
                    log.info("getDailyG6");
                    updateTotal();
                    isStart1G6[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getData(Object object, String site) throws Exception{
        HashMap dataMap = (HashMap) object;
        dataMap.forEach((key, value) -> {
            HashMap hashMap = (HashMap) value;
            Map<String,TotalDaily> mapTotal = new HashMap<>();
            BigDecimal chartdailygrid = BigDecimal.ZERO;
            BigDecimal chartdailyinv1pv = BigDecimal.ZERO;
            BigDecimal chartdailyinv1solar = BigDecimal.ZERO;
            BigDecimal chartdailyinv2pv = BigDecimal.ZERO;
            BigDecimal chartdailyinv2solar = BigDecimal.ZERO;
            BigDecimal chartdailyinv3pv = BigDecimal.ZERO;
            BigDecimal chartdailyinv3solar = BigDecimal.ZERO;
            BigDecimal chartdailyinv4pv = BigDecimal.ZERO;
            BigDecimal chartdailyinv4solar = BigDecimal.ZERO;
            BigDecimal chartdailyinv5pv = BigDecimal.ZERO;
            BigDecimal chartdailyinv5solar = BigDecimal.ZERO;
            BigDecimal chartdailyload = BigDecimal.ZERO;
            BigDecimal chartdailypv = BigDecimal.ZERO;
            BigDecimal chartdailysolar = BigDecimal.ZERO;
            String date = "";
            Object date_obj = hashMap.get("date");
            if(date_obj != null){
                date = (String)date_obj;
            }
            Object obj = hashMap.get("chartdailygrid");
            if(obj != null){
                chartdailygrid = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv1pv");
            if(obj != null){
                chartdailyinv1pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv1solar");
            if(obj != null){
                chartdailyinv1solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv2pv");
            if(obj != null){
                chartdailyinv2pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv2solar");
            if(obj != null){
                chartdailyinv2solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv3pv");
            if(obj != null){
                chartdailyinv3pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv3solar");
            if(obj != null){
                chartdailyinv3solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv4pv");
            if(obj != null){
                chartdailyinv4pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv4solar");
            if(obj != null){
                chartdailyinv4solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv5pv");
            if(obj != null){
                chartdailyinv5pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyinv5solar");
            if(obj != null){
                chartdailyinv5solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailyload");
            if(obj != null){
                chartdailyload = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailypv");
            if(obj != null){
                chartdailypv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartdailysolar");
            if(obj != null){
                chartdailysolar = convertObjectToBigDecimal(obj);
            }

            // chartdailygrid
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalgrid(chartdailygrid.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalgrid(chartdailygrid.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalgrid(new BigDecimal(totalDaily.getChartdailyTotalgrid()==null?"0":totalDaily.getChartdailyTotalgrid()).add(chartdailygrid).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv1pv
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv1pv(chartdailyinv1pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv1pv(chartdailyinv1pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv1pv(new BigDecimal(totalDaily.getChartdailyTotalinv1pv()==null?"0":totalDaily.getChartdailyTotalinv1pv()).add(chartdailyinv1pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv1solar
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv1solar(chartdailyinv1solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv1solar(chartdailyinv1solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv1solar(new BigDecimal(totalDaily.getChartdailyTotalinv1solar()==null?"0":totalDaily.getChartdailyTotalinv1solar()).add(chartdailyinv1solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv2pv
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv2pv(chartdailyinv2pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv2pv(chartdailyinv2pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv2pv(new BigDecimal(totalDaily.getChartdailyTotalinv2pv()==null?"0":totalDaily.getChartdailyTotalinv2pv()).add(chartdailyinv2pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv2solar
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv2solar(chartdailyinv2solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv2solar(chartdailyinv2solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv2solar(new BigDecimal(totalDaily.getChartdailyTotalinv2solar()==null?"0":totalDaily.getChartdailyTotalinv2solar()).add(chartdailyinv2solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv3pv
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv3pv(chartdailyinv3pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv3pv(chartdailyinv3pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv3pv(new BigDecimal(totalDaily.getChartdailyTotalinv3pv()==null?"0":totalDaily.getChartdailyTotalinv3pv()).add(chartdailyinv3pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv3solar
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv3solar(chartdailyinv3solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv3solar(chartdailyinv3solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv3solar(new BigDecimal(totalDaily.getChartdailyTotalinv3solar()==null?"0":totalDaily.getChartdailyTotalinv3solar()).add(chartdailyinv3solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv4pv
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv4pv(chartdailyinv4pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv4pv(chartdailyinv4pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv4pv(new BigDecimal(totalDaily.getChartdailyTotalinv4pv()==null?"0":totalDaily.getChartdailyTotalinv4pv()).add(chartdailyinv4pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv4solar
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv4solar(chartdailyinv4solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv4solar(chartdailyinv4solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv4solar(new BigDecimal(totalDaily.getChartdailyTotalinv4solar()==null?"0":totalDaily.getChartdailyTotalinv4solar()).add(chartdailyinv4solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv5pv
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv5pv(chartdailyinv5pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv5pv(chartdailyinv5pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv5pv(new BigDecimal(totalDaily.getChartdailyTotalinv5pv()==null?"0":totalDaily.getChartdailyTotalinv5pv()).add(chartdailyinv5pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyinv5solar
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalinv5solar(chartdailyinv5solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalinv5solar(chartdailyinv5solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalinv5solar(new BigDecimal(totalDaily.getChartdailyTotalinv5solar()==null?"0":totalDaily.getChartdailyTotalinv5solar()).add(chartdailyinv5solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailyload
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalload(chartdailyload.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalload(chartdailyload.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalload(new BigDecimal(totalDaily.getChartdailyTotalload()==null?"0":totalDaily.getChartdailyTotalload()).add(chartdailyload).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailypv
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalpv(chartdailypv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalpv(chartdailypv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalpv(new BigDecimal(totalDaily.getChartdailyTotalpv()==null?"0":totalDaily.getChartdailyTotalpv()).add(chartdailypv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
            // chartdailysolar
            if(dailyTotal.get(site) == null){
                TotalDaily totalDaily = new TotalDaily();
                totalDaily.setChartdailyTotalsolar(chartdailysolar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalDaily.setDate(date);
                mapTotal.put(date,totalDaily);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalDaily totalDaily = new TotalDaily();
                    totalDaily.setChartdailyTotalsolar(chartdailysolar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }else{
                    TotalDaily totalDaily = (TotalDaily)map.get(date);
                    totalDaily.setChartdailyTotalsolar(new BigDecimal(totalDaily.getChartdailyTotalsolar()==null?"0":totalDaily.getChartdailyTotalsolar()).add(chartdailysolar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalDaily.setDate(date);
                    map.put(date,totalDaily);
                    dailyTotal.put(site,map);
                }
            }
        });
    }

    public void updateTotal(){
        DatabaseReference refTotal = database.getReference("ParameterDailyTotal");
        log.info("updateTotal");
        try {
            refTotal.setValueAsync(dailyTotal);
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public BigDecimal convertObjectToBigDecimal(Object object) {
        BigDecimal result = BigDecimal.ZERO;
        if(object instanceof BigDecimal){
            result = (BigDecimal)object;
        }else if(object instanceof String){
            result = new BigDecimal((String)object);
        }else if(object instanceof Long){
            result = new BigDecimal((Long)object);
        }else if(object instanceof Integer){
            result = new BigDecimal((Integer)object);
        }else if(object instanceof Float){
            result = BigDecimal.valueOf((Float)object);
        }else if(object instanceof Double){
            result = BigDecimal.valueOf((Double)object);
        }else{
            log.warn("not convertObjectToBigDecimal {}",object.getClass());
        }
        return result.setScale(1, BigDecimal.ROUND_HALF_UP);
    }

}
