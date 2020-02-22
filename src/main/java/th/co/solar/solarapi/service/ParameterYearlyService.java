package th.co.solar.solarapi.service;

import com.google.firebase.database.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import th.co.solar.solarapi.model.TotalMonthly;
import th.co.solar.solarapi.model.TotalYearly;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ParameterYearlyService {

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

        dailyG1 = database.getReference("ParameterYearlyG1");
        dailyG2 = database.getReference("ParameterYearlyG2");
        dailyG3 = database.getReference("ParameterYearlyG3");
        dailyG4 = database.getReference("ParameterYearlyG4");
        dailyG5 = database.getReference("ParameterYearlyG5");
        dailyG6 = database.getReference("ParameterYearlyG6");

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
                    log.info("getYearlyG1");
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
                    log.info("getYearlyG2");
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
                    log.info("getYearlyG3");
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
                    log.info("getYearlyG4");
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
                    log.info("getYearlyG5");
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
                    log.info("getYearlyG6");
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
            Map<String,TotalYearly> mapTotal = new HashMap<>();
            BigDecimal chartyearlygrid = BigDecimal.ZERO;
            BigDecimal chartyearlyinv1pv = BigDecimal.ZERO;
            BigDecimal chartyearlyinv1solar = BigDecimal.ZERO;
            BigDecimal chartyearlyinv2pv = BigDecimal.ZERO;
            BigDecimal chartyearlyinv2solar = BigDecimal.ZERO;
            BigDecimal chartyearlyinv3pv = BigDecimal.ZERO;
            BigDecimal chartyearlyinv3solar = BigDecimal.ZERO;
            BigDecimal chartyearlyinv4pv = BigDecimal.ZERO;
            BigDecimal chartyearlyinv4solar = BigDecimal.ZERO;
            BigDecimal chartyearlyinv5pv = BigDecimal.ZERO;
            BigDecimal chartyearlyinv5solar = BigDecimal.ZERO;
            BigDecimal chartyearlyload = BigDecimal.ZERO;
            BigDecimal chartyearlypv = BigDecimal.ZERO;
            BigDecimal chartyearlysolar = BigDecimal.ZERO;
            String date = "";
            Object date_obj = hashMap.get("date");
            if(date_obj != null){
                date = (String)date_obj;
            }
            Object obj = hashMap.get("chartyearlygrid");
            if(obj != null){
                chartyearlygrid = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv1pv");
            if(obj != null){
                chartyearlyinv1pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv1solar");
            if(obj != null){
                chartyearlyinv1solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv2pv");
            if(obj != null){
                chartyearlyinv2pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv2solar");
            if(obj != null){
                chartyearlyinv2solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv3pv");
            if(obj != null){
                chartyearlyinv3pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv3solar");
            if(obj != null){
                chartyearlyinv3solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv4pv");
            if(obj != null){
                chartyearlyinv4pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv4solar");
            if(obj != null){
                chartyearlyinv4solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv5pv");
            if(obj != null){
                chartyearlyinv5pv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyinv5solar");
            if(obj != null){
                chartyearlyinv5solar = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlyload");
            if(obj != null){
                chartyearlyload = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlypv");
            if(obj != null){
                chartyearlypv = convertObjectToBigDecimal(obj);
            }
            obj = hashMap.get("chartyearlysolar");
            if(obj != null){
                chartyearlysolar = convertObjectToBigDecimal(obj);
            }

            // chartyearlygrid
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalgrid(chartyearlygrid.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalgrid(chartyearlygrid.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalgrid(new BigDecimal(totalYearly.getChartyearlyTotalgrid()==null?"0":totalYearly.getChartyearlyTotalgrid()).add(chartyearlygrid).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv1pv
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv1pv(chartyearlyinv1pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv1pv(chartyearlyinv1pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv1pv(new BigDecimal(totalYearly.getChartyearlyTotalinv1pv()==null?"0":totalYearly.getChartyearlyTotalinv1pv()).add(chartyearlyinv1pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv1solar
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv1solar(chartyearlyinv1solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv1solar(chartyearlyinv1solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv1solar(new BigDecimal(totalYearly.getChartyearlyTotalinv1solar()==null?"0":totalYearly.getChartyearlyTotalinv1solar()).add(chartyearlyinv1solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv2pv
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv2pv(chartyearlyinv2pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv2pv(chartyearlyinv2pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv2pv(new BigDecimal(totalYearly.getChartyearlyTotalinv2pv()==null?"0":totalYearly.getChartyearlyTotalinv2pv()).add(chartyearlyinv2pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv2solar
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv2solar(chartyearlyinv2solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv2solar(chartyearlyinv2solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv2solar(new BigDecimal(totalYearly.getChartyearlyTotalinv2solar()==null?"0":totalYearly.getChartyearlyTotalinv2solar()).add(chartyearlyinv2solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv3pv
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv3pv(chartyearlyinv3pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv3pv(chartyearlyinv3pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv3pv(new BigDecimal(totalYearly.getChartyearlyTotalinv3pv()==null?"0":totalYearly.getChartyearlyTotalinv3pv()).add(chartyearlyinv3pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv3solar
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv3solar(chartyearlyinv3solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv3solar(chartyearlyinv3solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv3solar(new BigDecimal(totalYearly.getChartyearlyTotalinv3solar()==null?"0":totalYearly.getChartyearlyTotalinv3solar()).add(chartyearlyinv3solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv4pv
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv4pv(chartyearlyinv4pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv4pv(chartyearlyinv4pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv4pv(new BigDecimal(totalYearly.getChartyearlyTotalinv4pv()==null?"0":totalYearly.getChartyearlyTotalinv4pv()).add(chartyearlyinv4pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv4solar
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv4solar(chartyearlyinv4solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv4solar(chartyearlyinv4solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv4solar(new BigDecimal(totalYearly.getChartyearlyTotalinv4solar()==null?"0":totalYearly.getChartyearlyTotalinv4solar()).add(chartyearlyinv4solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv5pv
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv5pv(chartyearlyinv5pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv5pv(chartyearlyinv5pv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv5pv(new BigDecimal(totalYearly.getChartyearlyTotalinv5pv()==null?"0":totalYearly.getChartyearlyTotalinv5pv()).add(chartyearlyinv5pv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyinv5solar
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalinv5solar(chartyearlyinv5solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalinv5solar(chartyearlyinv5solar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalinv5solar(new BigDecimal(totalYearly.getChartyearlyTotalinv5solar()==null?"0":totalYearly.getChartyearlyTotalinv5solar()).add(chartyearlyinv5solar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlyload
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalload(chartyearlyload.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalload(chartyearlyload.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalload(new BigDecimal(totalYearly.getChartyearlyTotalload()==null?"0":totalYearly.getChartyearlyTotalload()).add(chartyearlyload).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlypv
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalpv(chartyearlypv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalpv(chartyearlypv.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalpv(new BigDecimal(totalYearly.getChartyearlyTotalpv()==null?"0":totalYearly.getChartyearlyTotalpv()).add(chartyearlypv).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
            // chartyearlysolar
            if(dailyTotal.get(site) == null){
                TotalYearly totalYearly = new TotalYearly();
                totalYearly.setChartyearlyTotalsolar(chartyearlysolar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                totalYearly.setDate(date);
                mapTotal.put(date,totalYearly);
                dailyTotal.put(site,mapTotal);
            }else{
                Map map = (HashMap)dailyTotal.get(site);
                if(map.get(date) == null){
                    TotalYearly totalYearly = new TotalYearly();
                    totalYearly.setChartyearlyTotalsolar(chartyearlysolar.setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }else{
                    TotalYearly totalYearly = (TotalYearly)map.get(date);
                    totalYearly.setChartyearlyTotalsolar(new BigDecimal(totalYearly.getChartyearlyTotalsolar()==null?"0":totalYearly.getChartyearlyTotalsolar()).add(chartyearlysolar).setScale(1,BigDecimal.ROUND_HALF_UP).toString());
                    totalYearly.setDate(date);
                    map.put(date,totalYearly);
                    dailyTotal.put(site,map);
                }
            }
        });
    }

    public void updateTotal(){
        DatabaseReference refTotal = database.getReference("ParameterYearlyTotal");
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
