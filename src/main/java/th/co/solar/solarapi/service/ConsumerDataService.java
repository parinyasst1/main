package th.co.solar.solarapi.service;

import com.google.firebase.database.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import th.co.solar.solarapi.model.TotalSite;

import java.math.BigDecimal;
import java.util.*;

@Service
@Slf4j
public class ConsumerDataService {

    // Get a reference to our posts
    FirebaseDatabase database = null;

    DatabaseReference namePath = null;

    DatabaseReference ref1g1 = null;
    DatabaseReference ref2g1 = null;
    DatabaseReference ref3g1 = null;

    DatabaseReference ref1g2 = null;
    DatabaseReference ref2g2 = null;
    DatabaseReference ref3g2 = null;

    DatabaseReference ref1g3 = null;
    DatabaseReference ref2g3 = null;
    DatabaseReference ref3g3 = null;

    DatabaseReference ref1g4 = null;
    DatabaseReference ref2g4 = null;
    DatabaseReference ref3g4 = null;

    DatabaseReference ref1g5 = null;
    DatabaseReference ref2g5 = null;
    DatabaseReference ref3g5 = null;

    DatabaseReference ref1g6 = null;
    DatabaseReference ref2g6 = null;
    DatabaseReference ref3g6 = null;

//    BigDecimal[] gridkwTall = {BigDecimal.ZERO};
//    BigDecimal[] LoadkwTall = {BigDecimal.ZERO};

    Map<String, BigDecimal> gridkwTall = new HashMap<>();
    Map<String, BigDecimal> LoadkwTall = new HashMap<>();

    boolean[] isStartG1 = {true};
    boolean[] isStartG2 = {true};
    boolean[] isStartG3 = {true};
    boolean[] isStartG4 = {true};
    boolean[] isStartG5 = {true};
    boolean[] isStartG6 = {true};

    List<String> siteList = new ArrayList<>();

    public Map<String, Object> processQueueTotal() {
        gridkwTall = new HashMap<>();
        LoadkwTall = new HashMap<>();

        database = FirebaseDatabase.getInstance();

        namePath = database.getReference("NamePath");

        ref1g1 = database.getReference("ParameterRealtime1G1");
        ref2g1 = database.getReference("ParameterRealtime2G1");
        ref3g1 = database.getReference("ParameterRealtime3G1");

        ref1g2 = database.getReference("ParameterRealtime1G2");
        ref2g2 = database.getReference("ParameterRealtime2G2");
        ref3g2 = database.getReference("ParameterRealtime3G2");

        ref1g3 = database.getReference("ParameterRealtime1G3");
        ref2g3 = database.getReference("ParameterRealtime2G3");
        ref3g3 = database.getReference("ParameterRealtime3G3");

        ref1g4 = database.getReference("ParameterRealtime1G4");
        ref2g4 = database.getReference("ParameterRealtime2G4");
        ref3g4 = database.getReference("ParameterRealtime3G4");

        ref1g5 = database.getReference("ParameterRealtime1G5");
        ref2g5 = database.getReference("ParameterRealtime2G5");
        ref3g5 = database.getReference("ParameterRealtime3G5");

        ref1g6 = database.getReference("ParameterRealtime1G6");
        ref2g6 = database.getReference("ParameterRealtime2G6");
        ref3g6 = database.getReference("ParameterRealtime3G6");

        namePath.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if(isStartG1[0]) {
                    siteList = (List<String>) dataSnapshot.getValue();
                    log.info("siteList : {}", siteList);
                    getDataChangeG1();
                    isStartG1[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });

        return null;
    }

    public void getDataChangeG1(){
        ref1g1.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
//                log.info("onDataChange : {}",dataSnapshot.getValue());
                String group = "G1";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData1(obj,site);
                    }
                }
                if(isStartG2[0]) {
                    getDataChangeG2();
                    isStartG2[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChangeG2(){
        ref1g2.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
//                log.info("onDataChange : {}",dataSnapshot.getValue());
                String group = "G2";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData1(obj,site);
                    }
                }
                if(isStartG3[0]) {
                    getDataChangeG3();
                    isStartG3[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChangeG3(){
        ref1g3.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
//                log.info("onDataChange : {}",dataSnapshot.getValue());
                String group = "G3";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData1(obj,site);
                    }
                }
                if(isStartG4[0]) {
                    getDataChangeG4();
                    isStartG4[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChangeG4(){
        ref1g4.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
//                log.info("onDataChange : {}",dataSnapshot.getValue());
                String group = "G4";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData1(obj,site);
                    }
                }
                if(isStartG5[0]) {
                    getDataChangeG5();
                    isStartG5[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChangeG5(){
        ref1g5.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
//                log.info("onDataChange : {}",dataSnapshot.getValue());
                String group = "G5";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData1(obj,site);
                    }
                }
                if(isStartG6[0]) {
                    getDataChangeG6();
                    isStartG6[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChangeG6(){
        ref1g6.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
//                log.info("onDataChange : {}",dataSnapshot.getValue());
                String group = "G6";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData1(obj,site);
                    }
                }
                log.info("gridkwTallFinal : {}", gridkwTall);
                log.info("LoadkwTallFinal : {}", LoadkwTall);
                updateTotal();
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void updateTotal(){
        DatabaseReference refTotal = database.getReference("ParameterTotal");
        Map<String, Object> mapSite = new HashMap<>();
        try {
            siteList.forEach(site -> {
                TotalSite totalSite = new TotalSite();
                if(gridkwTall.get(site) != null){
                    totalSite.setGridkwTall(gridkwTall.get(site).toString());
                }
                if(LoadkwTall.get(site) != null){
                    totalSite.setLoadkwTall(LoadkwTall.get(site).toString());
                }
                mapSite.put(site,totalSite);
            });
//            log.info("mapSite : {}", mapSite);
            refTotal.setValueAsync(mapSite);
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public void getData1(Object object, String site){
//        log.info("object : {}", object);
//        log.info("site : {}", site);
        HashMap dataMap = (HashMap) object;
        BigDecimal gridkwT = BigDecimal.ZERO;
        BigDecimal LoadkwT = BigDecimal.ZERO;

        Object gridkwT_obj = dataMap.get("gridkwT");
        if(gridkwT_obj != null){
            gridkwT = convertObjectToBigDecimal(gridkwT_obj);
        }
        Object LoadkwT_obj = dataMap.get("LoadkwT");
        if(LoadkwT_obj != null){
            LoadkwT = convertObjectToBigDecimal(LoadkwT_obj);
        }

//        log.info("gridkwT : {}", gridkwT);
//        log.info("LoadkwT : {}", LoadkwT);

        Object gridkwTall_obj = gridkwTall.get(site);
        if(gridkwTall_obj != null){
            BigDecimal gridkwTallResult = convertObjectToBigDecimal(gridkwTall_obj);
            gridkwTallResult = gridkwTallResult.add(gridkwT);
            gridkwTall.put(site,gridkwTallResult);
        }else{
            gridkwTall.put(site,gridkwT);
        }

        Object LoadkwTall_obj = LoadkwTall.get(site);
        if(LoadkwTall_obj != null){
            BigDecimal LoadkwTallResult = convertObjectToBigDecimal(LoadkwTall_obj);
            LoadkwTallResult = LoadkwTallResult.add(LoadkwT);
            LoadkwTall.put(site,LoadkwTallResult);
        }else{
            LoadkwTall.put(site,LoadkwT);
        }

        log.info("gridkwTall : {}", gridkwTall);
        log.info("LoadkwTall : {}", LoadkwTall);

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
            result = new BigDecimal((Float)object);
        }else if(object instanceof Double){
            result = new BigDecimal((Double)object);
        }else if(object instanceof Integer){
            result = new BigDecimal((Integer)object);
        }else{
            log.warn("not convertObjectToBigDecimal {}",object.getClass());
        }
        return result.setScale(1, BigDecimal.ROUND_HALF_UP);
    }

}
