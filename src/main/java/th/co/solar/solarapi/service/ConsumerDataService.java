package th.co.solar.solarapi.service;

import com.google.firebase.database.*;
import io.swagger.models.auth.In;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import th.co.solar.solarapi.model.TotalSite;

import java.math.BigDecimal;
import java.math.RoundingMode;
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

    Map<String, BigDecimal> gridkwTall = new HashMap<>();
    Map<String, BigDecimal> LoadkwTall = new HashMap<>();

    Map<String, BigDecimal> solartotalinputall = new HashMap<>();
    Map<String, BigDecimal> solartotaloutputall = new HashMap<>();
    Map<String, BigDecimal> persengridall = new HashMap<>();
    Map<String, BigDecimal> persenpvall = new HashMap<>();
    Map<String, BigDecimal> persensolarall = new HashMap<>();

    Map<String, Integer> persengrid_gruop = new HashMap<>();
    Map<String, Integer> persenpv_gruop = new HashMap<>();
    Map<String, Integer> persensolar_gruop = new HashMap<>();

    boolean[] isName = {true};

    boolean[] isStart1G1 = {true};
    boolean[] isStart1G2 = {true};
    boolean[] isStart1G3 = {true};
    boolean[] isStart1G4 = {true};
    boolean[] isStart1G5 = {true};
    boolean[] isStart1G6 = {true};

    boolean[] isStart2G1 = {true};
    boolean[] isStart2G2 = {true};
    boolean[] isStart2G3 = {true};
    boolean[] isStart2G4 = {true};
    boolean[] isStart2G5 = {true};
    boolean[] isStart2G6 = {true};

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
                if(isName[0]) {
                    siteList = (List<String>) dataSnapshot.getValue();
                    log.info("siteList : {}", siteList);
                    getDataChange1G1();
                    isName[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });

        return null;
    }

    ////////////////////////////////////// TODO getDataChange1

    public void getDataChange1G1(){
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
                if(isStart1G1[0]) {
                    getDataChange1G2();
                    isStart1G1[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange1G2(){
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
                if(isStart1G2[0]) {
                    getDataChange1G3();
                    isStart1G2[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange1G3(){
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
                if(isStart1G3[0]) {
                    getDataChange1G4();
                    isStart1G3[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange1G4(){
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
                if(isStart1G4[0]) {
                    getDataChange1G5();
                    isStart1G4[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange1G5(){
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
                if(isStart1G5[0]) {
                    getDataChange1G6();
                    isStart1G5[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange1G6(){
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
                if(isStart1G6[0]) {
                    getDataChange2G1();
                    isStart1G6[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    ////////////////////////////////////// TODO getDataChange2

    public void getDataChange2G1(){
        ref2g1.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G1";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData2(obj,site);
                    }
                }
                if(isStart2G1[0]) {
                    getDataChange2G2();
                    isStart2G1[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange2G2(){
        ref2g2.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G2";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData2(obj,site);
                    }
                }
                if(isStart2G2[0]) {
                    getDataChange2G3();
                    isStart2G2[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange2G3(){
        ref2g3.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G3";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData2(obj,site);
                    }
                }
                if(isStart2G3[0]) {
                    getDataChange2G4();
                    isStart2G3[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange2G4(){
        ref2g4.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G4";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData2(obj,site);
                    }
                }
                if(isStart2G4[0]) {
                    getDataChange2G5();
                    isStart2G4[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange2G5(){
        ref2g5.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G5";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData2(obj,site);
                    }
                }
                if(isStart2G5[0]) {
                    getDataChange2G6();
                    isStart2G5[0] = false;
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChange2G6(){
        ref2g6.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                String group = "G6";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
//                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData2(obj,site);
                    }
                }
                if(isStart2G6[0]) {
                    updateTotal();
                    isStart2G6[0] = false;
                }
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
            log.info("gridkwTallFinal : {}", gridkwTall);
            log.info("LoadkwTallFinal : {}", LoadkwTall);
            log.info("solartotalinputallFinal : {}", solartotalinputall);
            log.info("solartotaloutputallFinal : {}", solartotaloutputall);
            log.info("persengridallFinal : {}", persengridall);
            log.info("persenpvallFinal : {}", persenpvall);
            log.info("persensolarallFinal : {}", persensolarall);
            log.info("persengrid_gruop : {}", persengrid_gruop);
            log.info("persenpv_gruop : {}", persenpv_gruop);
            log.info("persensolar_gruop : {}", persensolar_gruop);

            siteList.forEach(site -> {
                TotalSite totalSite = new TotalSite();
                // 1
                if(gridkwTall.get(site) != null){
                    totalSite.setGridkwTall(gridkwTall.get(site).toString());
                }
                if(LoadkwTall.get(site) != null){
                    totalSite.setLoadkwTall(LoadkwTall.get(site).toString());
                }
                // 2
                if(solartotalinputall.get(site) != null){
                    totalSite.setSolartotalinputall(solartotalinputall.get(site).toString());
                }
                if(solartotaloutputall.get(site) != null){
                    totalSite.setSolartotaloutputall(solartotaloutputall.get(site).toString());
                }
                if(persengridall.get(site) != null && persengrid_gruop.get(site) != null){
                    Integer group = persengrid_gruop.get(site);
                    BigDecimal result = persengridall.get(site).divide(new BigDecimal(group),1, RoundingMode.HALF_UP);
                    totalSite.setPersengridall(result.toString());
                }
                if(persenpvall.get(site) != null && persenpv_gruop.get(site) != null){
                    Integer group = persenpv_gruop.get(site);
                    BigDecimal result = persenpvall.get(site).divide(new BigDecimal(group),1, RoundingMode.HALF_UP);
                    totalSite.setPersenpvall(result.toString());
                }
                if(persensolarall.get(site) != null && persensolar_gruop.get(site) != null){
                    Integer group = persensolar_gruop.get(site);
                    BigDecimal result = persensolarall.get(site).divide(new BigDecimal(group),1, RoundingMode.HALF_UP);
                    totalSite.setPersensolarall(result.toString());
                }
                // 3
                mapSite.put(site,totalSite);
            });
//            log.info("mapSite : {}", mapSite);
            refTotal.setValueAsync(mapSite);
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public void getData1(Object object, String site){
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
    }

    public void getData2(Object object, String site){
        HashMap dataMap = (HashMap) object;

        BigDecimal solartotalinput = BigDecimal.ZERO;
        BigDecimal solartotaloutput = BigDecimal.ZERO;
        BigDecimal persengrid = BigDecimal.ZERO;
        BigDecimal persenpv = BigDecimal.ZERO;
        BigDecimal persensolar = BigDecimal.ZERO;

        Object solartotalinput_obj = dataMap.get("solartotalinput");
        if(solartotalinput_obj != null){
            solartotalinput = convertObjectToBigDecimal(solartotalinput_obj);
        }
        Object solartotaloutput_obj = dataMap.get("solartotaloutput");
        if(solartotaloutput_obj != null){
            solartotaloutput = convertObjectToBigDecimal(solartotaloutput_obj);
        }
        Object persengrid_obj = dataMap.get("persengrid");
        if(persengrid_obj != null){
            persengrid = convertObjectToBigDecimal(persengrid_obj);
            if(persengrid.compareTo(BigDecimal.ZERO) > 0){
                persengrid_gruop.merge(site, 1, Integer::sum);
            }
        }
        Object persenpv_obj = dataMap.get("persenpv");
        if(persenpv_obj != null){
            persenpv = convertObjectToBigDecimal(persenpv_obj);
            if(persenpv.compareTo(BigDecimal.ZERO) > 0){
                persenpv_gruop.merge(site, 1, Integer::sum);
            }
        }
        Object persensolar_obj = dataMap.get("persensolar");
        if(persensolar_obj != null){
            persensolar = convertObjectToBigDecimal(persensolar_obj);
            if(persensolar.compareTo(BigDecimal.ZERO) > 0){
                persensolar_gruop.merge(site, 1, Integer::sum);
            }
        }

        Object solartotalinputall_obj = solartotalinputall.get(site);
        if(solartotalinputall_obj != null){
            BigDecimal solartotalinputall_result = convertObjectToBigDecimal(solartotalinputall_obj);
            solartotalinputall_result = solartotalinputall_result.add(solartotalinput);
            solartotalinputall.put(site,solartotalinputall_result);
        }else{
            solartotalinputall.put(site,solartotalinput);
        }

        Object solartotaloutputall_obj = solartotaloutputall.get(site);
        if(solartotaloutputall_obj != null){
            BigDecimal solartotaloutputall_result = convertObjectToBigDecimal(solartotaloutputall_obj);
            solartotaloutputall_result = solartotaloutputall_result.add(solartotaloutput);
            solartotaloutputall.put(site,solartotaloutputall_result);
        }else{
            solartotaloutputall.put(site,solartotaloutput);
        }

        Object persengridall_obj = persengridall.get(site);
        if(persengridall_obj != null){
            BigDecimal persengridall_result = convertObjectToBigDecimal(persengridall_obj);
            persengridall_result = persengridall_result.add(persengrid);
            persengridall.put(site,persengridall_result);
        }else{
            persengridall.put(site,persengrid);
        }

        Object persenpvall_obj = persenpvall.get(site);
        if(persenpvall_obj != null){
            BigDecimal persenpvall_result = convertObjectToBigDecimal(persenpvall_obj);
            persenpvall_result = persenpvall_result.add(persenpv);
            persenpvall.put(site,persenpvall_result);
        }else{
            persenpvall.put(site,persenpv);
        }

        Object persensolarall_obj = persensolarall.get(site);
        if(persensolarall_obj != null){
            BigDecimal persensolarall_result = convertObjectToBigDecimal(persensolarall_obj);
            persensolarall_result = persensolarall_result.add(persensolar);
            persensolarall.put(site,persensolarall_result);
        }else{
            persensolarall.put(site,persensolar);
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
