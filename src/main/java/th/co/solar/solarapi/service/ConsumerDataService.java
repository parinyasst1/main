package th.co.solar.solarapi.service;

import com.google.firebase.database.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import th.co.solar.solarapi.model.WeatherForecast7Days;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Service
@Slf4j
public class ConsumerDataService {

    // Get a reference to our posts
    FirebaseDatabase database = null;
    DatabaseReference refTotal = null;

    DatabaseReference namePath = null;

    DatabaseReference ref1g1 = null;
    DatabaseReference ref2g1 = null;
    DatabaseReference ref3g1 = null;

    DatabaseReference ref1g2 = null;
    DatabaseReference ref2g2 = null;
    DatabaseReference ref3g2 = null;

    final BigDecimal[] gridkwTall = {BigDecimal.ZERO};
    final BigDecimal[] LoadkwTall = {BigDecimal.ZERO};

    List<String> siteList = new ArrayList<>();

    public Map<String, Object> processQueue() {
        database = FirebaseDatabase.getInstance();

        refTotal = database.getReference("ParameterTotal");

        namePath = database.getReference("NamePath");

        ref1g1 = database.getReference("ParameterRealtime1G1");
        ref2g1 = database.getReference("ParameterRealtime2G1");
        ref3g1 = database.getReference("ParameterRealtime3G1");

        ref1g2 = database.getReference("ParameterRealtime1G2");
        ref2g2 = database.getReference("ParameterRealtime2G2");
        ref3g2 = database.getReference("ParameterRealtime3G2");

        namePath.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                siteList = (List<String>) dataSnapshot.getValue();
                log.info("siteList : {}", siteList);
                getDataChangeG1();
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
                log.info("onDataChange : {}",dataSnapshot.getValue());
                String group = "G1";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData(obj);
                    }
                }
                getDataChangeG2();
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getDataChangeG2(){
        ref1g1.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                log.info("onDataChange : {}",dataSnapshot.getValue());
                String group = "G2";
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String site:siteList){
                    String key = site+group;
                    log.info("key : {}", key);
                    Object obj = hashMapData.get(key);
                    if(obj != null){
                        getData(obj);
                    }
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });
    }

    public void getData(Object object){
        log.info("object : {}", object);
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

        gridkwTall[0] = gridkwTall[0].add(gridkwT);
        LoadkwTall[0] = LoadkwTall[0].add(LoadkwT);

        log.info("gridkwTall : {}", gridkwTall[0]);
        log.info("LoadkwTall : {}", LoadkwTall[0]);

    }

    public BigDecimal convertObjectToBigDecimal(Object object){
        BigDecimal result = BigDecimal.ZERO;
        if(object instanceof String){
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
