package th.co.solar.solarapi.service;

import com.google.firebase.database.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import th.co.solar.solarapi.model.TotalDaily;
import th.co.solar.solarapi.model.TotalMonthly;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ParameterMonthlyService {

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

        dailyG1 = database.getReference("ParameterMonthlyG1");
        dailyG2 = database.getReference("ParameterMonthlyG2");
        dailyG3 = database.getReference("ParameterMonthlyG3");
        dailyG4 = database.getReference("ParameterMonthlyG4");
        dailyG5 = database.getReference("ParameterMonthlyG5");
        dailyG6 = database.getReference("ParameterMonthlyG6");

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
                    log.info("getMonthlyG1");
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
                    log.info("getMonthlyG2");
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
                    log.info("getMonthlyG3");
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
                    log.info("getMonthlyG4");
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
                    log.info("getMonthlyG5");
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
                    log.info("getMonthlyG6");
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
            Map<String,TotalMonthly> mapTotal = new HashMap<>();
            BigDecimal chartmonthlygrid = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv1pv = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv1solar = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv2pv = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv2solar = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv3pv = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv3solar = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv4pv = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv4solar = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv5pv = BigDecimal.ZERO;
            BigDecimal chartmonthlyinv5solar = BigDecimal.ZERO;
            BigDecimal chartmonthlyload = BigDecimal.ZERO;
            BigDecimal chartmonthlypv = BigDecimal.ZERO;
            BigDecimal chartmonthlysolar = BigDecimal.ZERO;
            String date = "";
            Object date_obj = hashMap.get("date");
            if(date_obj != null){
                date = (String)date_obj;
            }
            Object obj = hashMap.get("chartmonthlygrid");
            if(obj != null){
                chartmonthlygrid = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv1pv");
            if(obj != null){
                chartmonthlyinv1pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv1solar");
            if(obj != null){
                chartmonthlyinv1solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv2pv");
            if(obj != null){
                chartmonthlyinv2pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv2solar");
            if(obj != null){
                chartmonthlyinv2solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv3pv");
            if(obj != null){
                chartmonthlyinv3pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv3solar");
            if(obj != null){
                chartmonthlyinv3solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv4pv");
            if(obj != null){
                chartmonthlyinv4pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv4solar");
            if(obj != null){
                chartmonthlyinv4solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv5pv");
            if(obj != null){
                chartmonthlyinv5pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyinv5solar");
            if(obj != null){
                chartmonthlyinv5solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlyload");
            if(obj != null){
                chartmonthlyload = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlypv");
            if(obj != null){
                chartmonthlypv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartmonthlysolar");
            if(obj != null){
                chartmonthlysolar = convertObjectToBigDecimal(obj);
            }

            // chartmonthlygrid
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalgrid(chartmonthlygrid.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalgrid(chartmonthlygrid.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalgrid(new BigDecimal(totalMonthly.getChartmonthlyTotalgrid()==null?"0":totalMonthly.getChartmonthlyTotalgrid()).add(chartmonthlygrid).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv1pv
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv1pv(chartmonthlyinv1pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv1pv(chartmonthlyinv1pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv1pv(new BigDecimal(totalMonthly.getChartmonthlyTotalinv1pv()==null?"0":totalMonthly.getChartmonthlyTotalinv1pv()).add(chartmonthlyinv1pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv1solar
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv1solar(chartmonthlyinv1solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv1solar(chartmonthlyinv1solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv1solar(new BigDecimal(totalMonthly.getChartmonthlyTotalinv1solar()==null?"0":totalMonthly.getChartmonthlyTotalinv1solar()).add(chartmonthlyinv1solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv2pv
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv2pv(chartmonthlyinv2pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv2pv(chartmonthlyinv2pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv2pv(new BigDecimal(totalMonthly.getChartmonthlyTotalinv2pv()==null?"0":totalMonthly.getChartmonthlyTotalinv2pv()).add(chartmonthlyinv2pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv2solar
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv2solar(chartmonthlyinv2solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv2solar(chartmonthlyinv2solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv2solar(new BigDecimal(totalMonthly.getChartmonthlyTotalinv2solar()==null?"0":totalMonthly.getChartmonthlyTotalinv2solar()).add(chartmonthlyinv2solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv3pv
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv3pv(chartmonthlyinv3pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv3pv(chartmonthlyinv3pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv3pv(new BigDecimal(totalMonthly.getChartmonthlyTotalinv3pv()==null?"0":totalMonthly.getChartmonthlyTotalinv3pv()).add(chartmonthlyinv3pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv3solar
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv3solar(chartmonthlyinv3solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv3solar(chartmonthlyinv3solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv3solar(new BigDecimal(totalMonthly.getChartmonthlyTotalinv3solar()==null?"0":totalMonthly.getChartmonthlyTotalinv3solar()).add(chartmonthlyinv3solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv4pv
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv4pv(chartmonthlyinv4pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv4pv(chartmonthlyinv4pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv4pv(new BigDecimal(totalMonthly.getChartmonthlyTotalinv4pv()==null?"0":totalMonthly.getChartmonthlyTotalinv4pv()).add(chartmonthlyinv4pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv4solar
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv4solar(chartmonthlyinv4solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv4solar(chartmonthlyinv4solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv4solar(new BigDecimal(totalMonthly.getChartmonthlyTotalinv4solar()==null?"0":totalMonthly.getChartmonthlyTotalinv4solar()).add(chartmonthlyinv4solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv5pv
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv5pv(chartmonthlyinv5pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv5pv(chartmonthlyinv5pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv5pv(new BigDecimal(totalMonthly.getChartmonthlyTotalinv5pv()==null?"0":totalMonthly.getChartmonthlyTotalinv5pv()).add(chartmonthlyinv5pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyinv5solar
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalinv5solar(chartmonthlyinv5solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalinv5solar(chartmonthlyinv5solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalinv5solar(new BigDecimal(totalMonthly.getChartmonthlyTotalinv5solar()==null?"0":totalMonthly.getChartmonthlyTotalinv5solar()).add(chartmonthlyinv5solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlyload
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalload(chartmonthlyload.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalload(chartmonthlyload.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalload(new BigDecimal(totalMonthly.getChartmonthlyTotalload()==null?"0":totalMonthly.getChartmonthlyTotalload()).add(chartmonthlyload).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlypv
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalpv(chartmonthlypv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalpv(chartmonthlypv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalpv(new BigDecimal(totalMonthly.getChartmonthlyTotalpv()==null?"0":totalMonthly.getChartmonthlyTotalpv()).add(chartmonthlypv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
            // chartmonthlysolar
            if(dailyTotal.get(site) == null){
                TotalMonthly totalMonthly = new TotalMonthly();
                totalMonthly.setChartmonthlyTotalsolar(chartmonthlysolar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalMonthly.setDate(date);
                mapTotal.put(date,totalMonthly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalMonthly totalMonthly = new TotalMonthly();
                    totalMonthly.setChartmonthlyTotalsolar(chartmonthlysolar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }else{
                    TotalMonthly totalMonthly = (TotalMonthly)map.get(date);
                    totalMonthly.setChartmonthlyTotalsolar(new BigDecimal(totalMonthly.getChartmonthlyTotalsolar()==null?"0":totalMonthly.getChartmonthlyTotalsolar()).add(chartmonthlysolar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalMonthly.setDate(date);
                    map.put(date,totalMonthly);
                    dailyTotal.put(site,map);
                }
            }
        });
    }

    public void updateTotal(){
        DatabaseReference refTotal = database.getReference("ParameterMonthlyTotal");
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
