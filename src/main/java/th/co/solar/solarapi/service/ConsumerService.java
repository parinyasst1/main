package th.co.solar.solarapi.service;

import com.google.firebase.database.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import th.co.solar.solarapi.model.WeatherForecast7Days;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class ConsumerService {

    private final BigDecimal group = new BigDecimal("5");

    public Map<String, Object> processQueueTest() {
        // Get a reference to our posts
        final FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference refTotal = database.getReference("ParameterTotalS1");

        DatabaseReference ref1s1g1 = database.getReference("ParameterRealtime1S1G1");

        ref1s1g1.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                log.info("onDataChange : {}",dataSnapshot.getValue());
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });

        return null;
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

    public Map<String, Object> processQueueTotal() {
        log.info("Start processQueueTotal at {}", new Date());
        final boolean[] isStartRef1s1g1 = {true};
        final boolean[] isStartRef1s1g2 = {true};
        final boolean[] isStartRef1s1g3 = {true};
        final boolean[] isStartRef1s1g4 = {true};
        final boolean[] isStartRef1s1g5 = {true};
        final boolean[] isStartRef1s1g6 = {true};

        final boolean[] isStartRef2s1g1 = {true};
        final boolean[] isStartRef2s1g2 = {true};
        final boolean[] isStartRef2s1g3 = {true};
        final boolean[] isStartRef2s1g4 = {true};
        final boolean[] isStartRef2s1g5 = {true};
        final boolean[] isStartRef2s1g6 = {true};

        final boolean[] isStartRef3s1g1 = {true};
        final boolean[] isStartRef3s1g2 = {true};
        final boolean[] isStartRef3s1g3 = {true};
        final boolean[] isStartRef3s1g4 = {true};
        final boolean[] isStartRef3s1g5 = {true};
        final boolean[] isStartRef3s1g6 = {true};


        final Map<String, Object> userUpdates = new HashMap<>();
try {


        final BigDecimal[] gridkwTall = {BigDecimal.ZERO};
        final BigDecimal[] LoadkwTall = {BigDecimal.ZERO};

        final BigDecimal[] solartotalinputall = {BigDecimal.ZERO};
        final BigDecimal[] solartotaloutputall = {BigDecimal.ZERO};
        final BigDecimal[] persengridall = {BigDecimal.ZERO};
        final BigDecimal[] persenpvall = {BigDecimal.ZERO};
        final BigDecimal[] persensolarall = {BigDecimal.ZERO};

        final BigDecimal[] griduseall = {BigDecimal.ZERO};
        final BigDecimal[] loadall = {BigDecimal.ZERO};
        final BigDecimal[] solartotalinputaccall = {BigDecimal.ZERO};
        final BigDecimal[] solartotaloutputaccall = {BigDecimal.ZERO};

        // Get a reference to our posts
        final FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference refTotal = database.getReference("ParameterTotalS1");

        DatabaseReference ref1s1g1 = database.getReference("ParameterRealtime1S1G1");
        DatabaseReference ref1s1g2 = database.getReference("ParameterRealtime1S1G2");
        DatabaseReference ref1s1g3 = database.getReference("ParameterRealtime1S1G3");
        DatabaseReference ref1s1g4 = database.getReference("ParameterRealtime1S1G4");
        DatabaseReference ref1s1g5 = database.getReference("ParameterRealtime1S1G5");
        DatabaseReference ref1s1g6 = database.getReference("ParameterRealtime1S1G6");

        DatabaseReference ref2s1g1 = database.getReference("ParameterRealtime2S1G1");
        DatabaseReference ref2s1g2 = database.getReference("ParameterRealtime2S1G2");
        DatabaseReference ref2s1g3 = database.getReference("ParameterRealtime2S1G3");
        DatabaseReference ref2s1g4 = database.getReference("ParameterRealtime2S1G4");
        DatabaseReference ref2s1g5 = database.getReference("ParameterRealtime2S1G5");
        DatabaseReference ref2s1g6 = database.getReference("ParameterRealtime2S1G6");

        DatabaseReference ref3s1g1 = database.getReference("ParameterRealtime3S1G1");
        DatabaseReference ref3s1g2 = database.getReference("ParameterRealtime3S1G2");
        DatabaseReference ref3s1g3 = database.getReference("ParameterRealtime3S1G3");
        DatabaseReference ref3s1g4 = database.getReference("ParameterRealtime3S1G4");
        DatabaseReference ref3s1g5 = database.getReference("ParameterRealtime3S1G5");
        DatabaseReference ref3s1g6 = database.getReference("ParameterRealtime3S1G6");

        // ParameterRealtime1S1G1
        ref1s1g1.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
//                log.info("isStartRef1s1g1 : {}",isStartRef1s1g1[0]);
//                log.info("dataSnapshot.exists : {}",dataSnapshot.exists());
                if(dataSnapshot.exists() && isStartRef1s1g1[0]){
                    HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                    Object obj = hashMapData.get("DataRealtime1S1G1");
                    if (obj == null) {
                        return;
                    }
                    HashMap dataMap = (HashMap) obj;
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
                    log.info("gridkwTs1g1 : {}", gridkwT);
//                    log.info("LoadkwTs1g1 : {}", LoadkwT);

                    gridkwTall[0] = gridkwTall[0].add(gridkwT);
                    LoadkwTall[0] = LoadkwTall[0].add(LoadkwT);
                    isStartRef1s1g1[0] = false;
                    // ParameterRealtime1S1G2
                    ref1s1g2.addValueEventListener(new ValueEventListener() {
                        @Override
                        public void onDataChange(DataSnapshot dataSnapshot) {
//                            log.info("isStartRef1s1g2 : {}",isStartRef1s1g2[0]);
                            if(dataSnapshot.exists() && isStartRef1s1g2[0]){
                                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                Object obj = hashMapData.get("DataRealtime1S1G2");
                                if (obj == null) {
                                    return;
                                }
                                HashMap dataMap = (HashMap) obj;
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
                                log.info("gridkwTs1g2 : {}", gridkwT);
//                                log.info("LoadkwTs1g2 : {}", LoadkwT);

                                gridkwTall[0] = gridkwTall[0].add(gridkwT);
                                LoadkwTall[0] = LoadkwTall[0].add(LoadkwT);
                                isStartRef1s1g2[0] = false;
                                // ParameterRealtime1S1G3
                                ref1s1g3.addValueEventListener(new ValueEventListener() {
                                    @Override
                                    public void onDataChange(DataSnapshot dataSnapshot) {
                                        if(dataSnapshot.exists() && isStartRef1s1g3[0]){
                                            HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                            Object obj = hashMapData.get("DataRealtime1S1G3");
                                            if (obj == null) {
                                                return;
                                            }
                                            HashMap dataMap = (HashMap) obj;
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
                                            log.info("gridkwTs1g3 : {}", gridkwT);
//                                            log.info("LoadkwTs1g3 : {}", LoadkwT);

                                            gridkwTall[0] = gridkwTall[0].add(gridkwT);
                                            LoadkwTall[0] = LoadkwTall[0].add(LoadkwT);
                                            isStartRef1s1g3[0] = false;
                                            // ParameterRealtime1S1G4
                                            ref1s1g4.addValueEventListener(new ValueEventListener() {
                                                @Override
                                                public void onDataChange(DataSnapshot dataSnapshot) {
                                                    if(dataSnapshot.exists() && isStartRef1s1g4[0]){
                                                        HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                        Object obj = hashMapData.get("DataRealtime1S1G4");
                                                        if (obj == null) {
                                                            return;
                                                        }
                                                        HashMap dataMap = (HashMap) obj;
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
                                                        log.info("gridkwTs1g4 : {}", gridkwT);
//                                                        log.info("LoadkwTs1g4 : {}", LoadkwT);

                                                        gridkwTall[0] = gridkwTall[0].add(gridkwT);
                                                        LoadkwTall[0] = LoadkwTall[0].add(LoadkwT);
                                                        isStartRef1s1g4[0] = false;
                                                        // ParameterRealtime1S1G5
                                                        ref1s1g5.addValueEventListener(new ValueEventListener() {
                                                            @Override
                                                            public void onDataChange(DataSnapshot dataSnapshot) {
                                                                if(dataSnapshot.exists() && isStartRef1s1g5[0]){
                                                                    HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                    Object obj = hashMapData.get("DataRealtime1S1G5");
                                                                    if (obj == null) {
                                                                        return;
                                                                    }
                                                                    HashMap dataMap = (HashMap) obj;
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
                                                                    log.info("gridkwTs1g5 : {}", gridkwT);
//                                                                    log.info("LoadkwTs1g5 : {}", LoadkwT);

                                                                    gridkwTall[0] = gridkwTall[0].add(gridkwT);
                                                                    LoadkwTall[0] = LoadkwTall[0].add(LoadkwT);
                                                                    isStartRef1s1g5[0] = false;
                                                                    // ParameterRealtime1S1G6
                                                                    ref1s1g6.addValueEventListener(new ValueEventListener() {
                                                                        @Override
                                                                        public void onDataChange(DataSnapshot dataSnapshot) {
                                                                            if(dataSnapshot.exists() && isStartRef1s1g6[0]){
                                                                                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                Object obj = hashMapData.get("DataRealtime1S1G6");
                                                                                if (obj == null) {
                                                                                    return;
                                                                                }
                                                                                HashMap dataMap = (HashMap) obj;
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
                                                                                log.info("gridkwTs1g6 : {}", gridkwT);
//                                                                                log.info("LoadkwTs1g6 : {}", LoadkwT);

                                                                                gridkwTall[0] = gridkwTall[0].add(gridkwT);
                                                                                LoadkwTall[0] = LoadkwTall[0].add(LoadkwT);

                                                                                log.info("gridkwTall : {}", gridkwTall[0]);
//                                                                                log.info("LoadkwTall : {}", LoadkwTall[0]);
                                                                                isStartRef1s1g6[0] = false;
                                                                                // ParameterRealtime2S1G1
                                                                                ref2s1g1.addValueEventListener(new ValueEventListener() {
                                                                                    @Override
                                                                                    public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                        if(dataSnapshot.exists() && isStartRef2s1g1[0]){
                                                                                            HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                            Object obj = hashMapData.get("DataRealtime2S1G1");
                                                                                            if (obj == null) {
                                                                                                return;
                                                                                            }
                                                                                            HashMap dataMap = (HashMap) obj;
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
                                                                                            if(solartotaloutput_obj != null){
                                                                                                persengrid = convertObjectToBigDecimal(persengrid_obj);
                                                                                            }
                                                                                            Object persenpv_obj = dataMap.get("persenpv");
                                                                                            if(persenpv_obj != null){
                                                                                                persenpv = convertObjectToBigDecimal(persenpv_obj);
                                                                                            }
                                                                                            Object persensolar_obj = dataMap.get("persensolar");
                                                                                            if(persensolar_obj != null){
                                                                                                persensolar = convertObjectToBigDecimal(persensolar_obj);
                                                                                            }

                                                                                            solartotalinputall[0] = solartotalinputall[0].add(solartotalinput);
                                                                                            solartotaloutputall[0] = solartotaloutputall[0].add(solartotaloutput);

                                                                                            persengridall[0] = persengridall[0].add(persengrid);
                                                                                            persenpvall[0] = persenpvall[0].add(persenpv);
                                                                                            persensolarall[0] = persensolarall[0].add(persensolar);

                                                                                            persengridall[0] = persengridall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                            persenpvall[0] = persenpvall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                            persensolarall[0] = persensolarall[0].divide(group,1, RoundingMode.HALF_UP);

                                                                                            isStartRef2s1g1[0] = false;
                                                                                            // ParameterRealtime2S1G2
                                                                                            ref2s1g2.addValueEventListener(new ValueEventListener() {
                                                                                                @Override
                                                                                                public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                    if(dataSnapshot.exists() && isStartRef2s1g2[0]){
                                                                                                        HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                        Object obj = hashMapData.get("DataRealtime2S1G2");
                                                                                                        if (obj == null) {
                                                                                                            return;
                                                                                                        }
                                                                                                        HashMap dataMap = (HashMap) obj;
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
                                                                                                        if(solartotaloutput_obj != null){
                                                                                                            persengrid = convertObjectToBigDecimal(persengrid_obj);
                                                                                                        }
                                                                                                        Object persenpv_obj = dataMap.get("persenpv");
                                                                                                        if(persenpv_obj != null){
                                                                                                            persenpv = convertObjectToBigDecimal(persenpv_obj);
                                                                                                        }
                                                                                                        Object persensolar_obj = dataMap.get("persensolar");
                                                                                                        if(persensolar_obj != null){
                                                                                                            persensolar = convertObjectToBigDecimal(persensolar_obj);
                                                                                                        }

                                                                                                        solartotalinputall[0] = solartotalinputall[0].add(solartotalinput);
                                                                                                        solartotaloutputall[0] = solartotaloutputall[0].add(solartotaloutput);

                                                                                                        persengridall[0] = persengridall[0].add(persengrid);
                                                                                                        persenpvall[0] = persenpvall[0].add(persenpv);
                                                                                                        persensolarall[0] = persensolarall[0].add(persensolar);

                                                                                                        persengridall[0] = persengridall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                        persenpvall[0] = persenpvall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                        persensolarall[0] = persensolarall[0].divide(group,1, RoundingMode.HALF_UP);

                                                                                                        isStartRef2s1g2[0] = false;
                                                                                                        // ParameterRealtime2S1G3
                                                                                                        ref2s1g3.addValueEventListener(new ValueEventListener() {
                                                                                                            @Override
                                                                                                            public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                if(dataSnapshot.exists() && isStartRef2s1g3[0]){
                                                                                                                    HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                    Object obj = hashMapData.get("DataRealtime2S1G3");
                                                                                                                    if (obj == null) {
                                                                                                                        return;
                                                                                                                    }
                                                                                                                    HashMap dataMap = (HashMap) obj;
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
                                                                                                                    if(solartotaloutput_obj != null){
                                                                                                                        persengrid = convertObjectToBigDecimal(persengrid_obj);
                                                                                                                    }
                                                                                                                    Object persenpv_obj = dataMap.get("persenpv");
                                                                                                                    if(persenpv_obj != null){
                                                                                                                        persenpv = convertObjectToBigDecimal(persenpv_obj);
                                                                                                                    }
                                                                                                                    Object persensolar_obj = dataMap.get("persensolar");
                                                                                                                    if(persensolar_obj != null){
                                                                                                                        persensolar = convertObjectToBigDecimal(persensolar_obj);
                                                                                                                    }

                                                                                                                    solartotalinputall[0] = solartotalinputall[0].add(solartotalinput);
                                                                                                                    solartotaloutputall[0] = solartotaloutputall[0].add(solartotaloutput);

                                                                                                                    persengridall[0] = persengridall[0].add(persengrid);
                                                                                                                    persenpvall[0] = persenpvall[0].add(persenpv);
                                                                                                                    persensolarall[0] = persensolarall[0].add(persensolar);

                                                                                                                    persengridall[0] = persengridall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                                    persenpvall[0] = persenpvall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                                    persensolarall[0] = persensolarall[0].divide(group,1, RoundingMode.HALF_UP);

                                                                                                                    isStartRef2s1g3[0] = false;
                                                                                                                    // ParameterRealtime2S1G4
                                                                                                                    ref2s1g4.addValueEventListener(new ValueEventListener() {
                                                                                                                        @Override
                                                                                                                        public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                            if(dataSnapshot.exists() && isStartRef2s1g4[0]){
                                                                                                                                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                Object obj = hashMapData.get("DataRealtime2S1G4");
                                                                                                                                if (obj == null) {
                                                                                                                                    return;
                                                                                                                                }
                                                                                                                                HashMap dataMap = (HashMap) obj;
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
                                                                                                                                if(solartotaloutput_obj != null){
                                                                                                                                    persengrid = convertObjectToBigDecimal(persengrid_obj);
                                                                                                                                }
                                                                                                                                Object persenpv_obj = dataMap.get("persenpv");
                                                                                                                                if(persenpv_obj != null){
                                                                                                                                    persenpv = convertObjectToBigDecimal(persenpv_obj);
                                                                                                                                }
                                                                                                                                Object persensolar_obj = dataMap.get("persensolar");
                                                                                                                                if(persensolar_obj != null){
                                                                                                                                    persensolar = convertObjectToBigDecimal(persensolar_obj);
                                                                                                                                }

                                                                                                                                solartotalinputall[0] = solartotalinputall[0].add(solartotalinput);
                                                                                                                                solartotaloutputall[0] = solartotaloutputall[0].add(solartotaloutput);

                                                                                                                                persengridall[0] = persengridall[0].add(persengrid);
                                                                                                                                persenpvall[0] = persenpvall[0].add(persenpv);
                                                                                                                                persensolarall[0] = persensolarall[0].add(persensolar);

                                                                                                                                persengridall[0] = persengridall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                                                persenpvall[0] = persenpvall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                                                persensolarall[0] = persensolarall[0].divide(group,1, RoundingMode.HALF_UP);

                                                                                                                                isStartRef2s1g4[0] = false;
                                                                                                                                // ParameterRealtime2S1G5
                                                                                                                                ref2s1g5.addValueEventListener(new ValueEventListener() {
                                                                                                                                    @Override
                                                                                                                                    public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                                        if(dataSnapshot.exists() && isStartRef2s1g5[0]){
                                                                                                                                            HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                            Object obj = hashMapData.get("DataRealtime2S1G5");
                                                                                                                                            if (obj == null) {
                                                                                                                                                return;
                                                                                                                                            }
                                                                                                                                            HashMap dataMap = (HashMap) obj;
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
                                                                                                                                            if(solartotaloutput_obj != null){
                                                                                                                                                persengrid = convertObjectToBigDecimal(persengrid_obj);
                                                                                                                                            }
                                                                                                                                            Object persenpv_obj = dataMap.get("persenpv");
                                                                                                                                            if(persenpv_obj != null){
                                                                                                                                                persenpv = convertObjectToBigDecimal(persenpv_obj);
                                                                                                                                            }
                                                                                                                                            Object persensolar_obj = dataMap.get("persensolar");
                                                                                                                                            if(persensolar_obj != null){
                                                                                                                                                persensolar = convertObjectToBigDecimal(persensolar_obj);
                                                                                                                                            }

                                                                                                                                            solartotalinputall[0] = solartotalinputall[0].add(solartotalinput);
                                                                                                                                            solartotaloutputall[0] = solartotaloutputall[0].add(solartotaloutput);

                                                                                                                                            persengridall[0] = persengridall[0].add(persengrid);
                                                                                                                                            persenpvall[0] = persenpvall[0].add(persenpv);
                                                                                                                                            persensolarall[0] = persensolarall[0].add(persensolar);

                                                                                                                                            persengridall[0] = persengridall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                                                            persenpvall[0] = persenpvall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                                                            persensolarall[0] = persensolarall[0].divide(group,1, RoundingMode.HALF_UP);

                                                                                                                                            isStartRef2s1g5[0] = false;
                                                                                                                                            // ParameterRealtime2S1G6
                                                                                                                                            ref2s1g6.addValueEventListener(new ValueEventListener() {
                                                                                                                                                @Override
                                                                                                                                                public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                                                    if(dataSnapshot.exists() && isStartRef2s1g6[0]){
                                                                                                                                                        HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                                        Object obj = hashMapData.get("DataRealtime2S1G6");
                                                                                                                                                        if (obj == null) {
                                                                                                                                                            return;
                                                                                                                                                        }
                                                                                                                                                        HashMap dataMap = (HashMap) obj;
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
                                                                                                                                                        if(solartotaloutput_obj != null){
                                                                                                                                                            persengrid = convertObjectToBigDecimal(persengrid_obj);
                                                                                                                                                        }
                                                                                                                                                        Object persenpv_obj = dataMap.get("persenpv");
                                                                                                                                                        if(persenpv_obj != null){
                                                                                                                                                            persenpv = convertObjectToBigDecimal(persenpv_obj);
                                                                                                                                                        }
                                                                                                                                                        Object persensolar_obj = dataMap.get("persensolar");
                                                                                                                                                        if(persensolar_obj != null){
                                                                                                                                                            persensolar = convertObjectToBigDecimal(persensolar_obj);
                                                                                                                                                        }

                                                                                                                                                        solartotalinputall[0] = solartotalinputall[0].add(solartotalinput);
                                                                                                                                                        solartotaloutputall[0] = solartotaloutputall[0].add(solartotaloutput);

                                                                                                                                                        persengridall[0] = persengridall[0].add(persengrid);
                                                                                                                                                        persenpvall[0] = persenpvall[0].add(persenpv);
                                                                                                                                                        persensolarall[0] = persensolarall[0].add(persensolar);

                                                                                                                                                        persengridall[0] = persengridall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                                                                        persenpvall[0] = persenpvall[0].divide(group,1, RoundingMode.HALF_UP);
                                                                                                                                                        persensolarall[0] = persensolarall[0].divide(group,1, RoundingMode.HALF_UP);

                                                                                                                                                        isStartRef2s1g6[0] = false;
                                                                                                                                                        // ParameterRealtime3S1G1
                                                                                                                                                        ref3s1g1.addValueEventListener(new ValueEventListener() {
                                                                                                                                                            @Override
                                                                                                                                                            public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                                                                if(dataSnapshot.exists() && isStartRef3s1g1[0]){
                                                                                                                                                                    HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                                                    Object obj = hashMapData.get("DataRealtime3S1G1");
                                                                                                                                                                    if (obj == null) {
                                                                                                                                                                        return;
                                                                                                                                                                    }
                                                                                                                                                                    HashMap dataMap = (HashMap) obj;
                                                                                                                                                                    BigDecimal griduse = BigDecimal.ZERO;
                                                                                                                                                                    BigDecimal load = BigDecimal.ZERO;
                                                                                                                                                                    BigDecimal solartotalinputacc = BigDecimal.ZERO;
                                                                                                                                                                    BigDecimal solartotaloutputacc = BigDecimal.ZERO;
                                                                                                                                                                    Object griduse_obj = dataMap.get("griduse");
                                                                                                                                                                    if(griduse_obj != null){
                                                                                                                                                                        griduse = convertObjectToBigDecimal(griduse_obj);
                                                                                                                                                                    }
                                                                                                                                                                    Object load_obj = dataMap.get("load");
                                                                                                                                                                    if(load_obj != null){
                                                                                                                                                                        load = convertObjectToBigDecimal(load_obj);
                                                                                                                                                                    }
                                                                                                                                                                    Object solartotalinputacc_obj = dataMap.get("solartotalinputacc");
                                                                                                                                                                    if(solartotalinputacc_obj != null){
                                                                                                                                                                        solartotalinputacc = convertObjectToBigDecimal(solartotalinputacc_obj);
                                                                                                                                                                    }
                                                                                                                                                                    Object solartotaloutputacc_obj = dataMap.get("solartotaloutputacc");
                                                                                                                                                                    if(solartotaloutputacc_obj != null){
                                                                                                                                                                        solartotaloutputacc = convertObjectToBigDecimal(solartotaloutputacc_obj);
                                                                                                                                                                    }

                                                                                                                                                                    griduseall[0] = griduseall[0].add(griduse);
                                                                                                                                                                    loadall[0] = loadall[0].add(load);
                                                                                                                                                                    solartotalinputaccall[0] = solartotalinputaccall[0].add(solartotalinputacc);
                                                                                                                                                                    solartotaloutputaccall[0] = solartotaloutputaccall[0].add(solartotaloutputacc);

                                                                                                                                                                    isStartRef3s1g1[0] = false;
                                                                                                                                                                    // ParameterRealtime3S1G2
                                                                                                                                                                    ref3s1g2.addValueEventListener(new ValueEventListener() {
                                                                                                                                                                        @Override
                                                                                                                                                                        public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                                                                            if(dataSnapshot.exists() && isStartRef3s1g2[0]){
                                                                                                                                                                                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                                                                Object obj = hashMapData.get("DataRealtime3S1G2");
                                                                                                                                                                                if (obj == null) {
                                                                                                                                                                                    return;
                                                                                                                                                                                }
                                                                                                                                                                                HashMap dataMap = (HashMap) obj;
                                                                                                                                                                                BigDecimal griduse = BigDecimal.ZERO;
                                                                                                                                                                                BigDecimal load = BigDecimal.ZERO;
                                                                                                                                                                                BigDecimal solartotalinputacc = BigDecimal.ZERO;
                                                                                                                                                                                BigDecimal solartotaloutputacc = BigDecimal.ZERO;
                                                                                                                                                                                Object griduse_obj = dataMap.get("griduse");
                                                                                                                                                                                if(griduse_obj != null){
                                                                                                                                                                                    griduse = convertObjectToBigDecimal(griduse_obj);
                                                                                                                                                                                }
                                                                                                                                                                                Object load_obj = dataMap.get("load");
                                                                                                                                                                                if(load_obj != null){
                                                                                                                                                                                    load = convertObjectToBigDecimal(load_obj);
                                                                                                                                                                                }
                                                                                                                                                                                Object solartotalinputacc_obj = dataMap.get("solartotalinputacc");
                                                                                                                                                                                if(solartotalinputacc_obj != null){
                                                                                                                                                                                    solartotalinputacc = convertObjectToBigDecimal(solartotalinputacc_obj);
                                                                                                                                                                                }
                                                                                                                                                                                Object solartotaloutputacc_obj = dataMap.get("solartotaloutputacc");
                                                                                                                                                                                if(solartotaloutputacc_obj != null){
                                                                                                                                                                                    solartotaloutputacc = convertObjectToBigDecimal(solartotaloutputacc_obj);
                                                                                                                                                                                }

                                                                                                                                                                                griduseall[0] = griduseall[0].add(griduse);
                                                                                                                                                                                loadall[0] = loadall[0].add(load);
                                                                                                                                                                                solartotalinputaccall[0] = solartotalinputaccall[0].add(solartotalinputacc);
                                                                                                                                                                                solartotaloutputaccall[0] = solartotaloutputaccall[0].add(solartotaloutputacc);


                                                                                                                                                                                isStartRef3s1g2[0] = false;
                                                                                                                                                                                // ParameterRealtime3S1G3
                                                                                                                                                                                ref3s1g3.addValueEventListener(new ValueEventListener() {
                                                                                                                                                                                    @Override
                                                                                                                                                                                    public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                                                                                        if(dataSnapshot.exists() && isStartRef3s1g3[0]){
                                                                                                                                                                                            HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                                                                            Object obj = hashMapData.get("DataRealtime3S1G3");
                                                                                                                                                                                            if (obj == null) {
                                                                                                                                                                                                return;
                                                                                                                                                                                            }
                                                                                                                                                                                            HashMap dataMap = (HashMap) obj;
                                                                                                                                                                                            BigDecimal griduse = BigDecimal.ZERO;
                                                                                                                                                                                            BigDecimal load = BigDecimal.ZERO;
                                                                                                                                                                                            BigDecimal solartotalinputacc = BigDecimal.ZERO;
                                                                                                                                                                                            BigDecimal solartotaloutputacc = BigDecimal.ZERO;
                                                                                                                                                                                            Object griduse_obj = dataMap.get("griduse");
                                                                                                                                                                                            if(griduse_obj != null){
                                                                                                                                                                                                griduse = convertObjectToBigDecimal(griduse_obj);
                                                                                                                                                                                            }
                                                                                                                                                                                            Object load_obj = dataMap.get("load");
                                                                                                                                                                                            if(load_obj != null){
                                                                                                                                                                                                load = convertObjectToBigDecimal(load_obj);
                                                                                                                                                                                            }
                                                                                                                                                                                            Object solartotalinputacc_obj = dataMap.get("solartotalinputacc");
                                                                                                                                                                                            if(solartotalinputacc_obj != null){
                                                                                                                                                                                                solartotalinputacc = convertObjectToBigDecimal(solartotalinputacc_obj);
                                                                                                                                                                                            }
                                                                                                                                                                                            Object solartotaloutputacc_obj = dataMap.get("solartotaloutputacc");
                                                                                                                                                                                            if(solartotaloutputacc_obj != null){
                                                                                                                                                                                                solartotaloutputacc = convertObjectToBigDecimal(solartotaloutputacc_obj);
                                                                                                                                                                                            }

                                                                                                                                                                                            griduseall[0] = griduseall[0].add(griduse);
                                                                                                                                                                                            loadall[0] = loadall[0].add(load);
                                                                                                                                                                                            solartotalinputaccall[0] = solartotalinputaccall[0].add(solartotalinputacc);
                                                                                                                                                                                            solartotaloutputaccall[0] = solartotaloutputaccall[0].add(solartotaloutputacc);


                                                                                                                                                                                            isStartRef3s1g3[0] = false;
                                                                                                                                                                                            // ParameterRealtime3S1G4
                                                                                                                                                                                            ref3s1g4.addValueEventListener(new ValueEventListener() {
                                                                                                                                                                                                @Override
                                                                                                                                                                                                public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                                                                                                    if(dataSnapshot.exists() && isStartRef3s1g4[0]){
                                                                                                                                                                                                        HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                                                                                        Object obj = hashMapData.get("DataRealtime3S1G4");
                                                                                                                                                                                                        if (obj == null) {
                                                                                                                                                                                                            return;
                                                                                                                                                                                                        }
                                                                                                                                                                                                        HashMap dataMap = (HashMap) obj;
                                                                                                                                                                                                        BigDecimal griduse = BigDecimal.ZERO;
                                                                                                                                                                                                        BigDecimal load = BigDecimal.ZERO;
                                                                                                                                                                                                        BigDecimal solartotalinputacc = BigDecimal.ZERO;
                                                                                                                                                                                                        BigDecimal solartotaloutputacc = BigDecimal.ZERO;
                                                                                                                                                                                                        Object griduse_obj = dataMap.get("griduse");
                                                                                                                                                                                                        if(griduse_obj != null){
                                                                                                                                                                                                            griduse = convertObjectToBigDecimal(griduse_obj);
                                                                                                                                                                                                        }
                                                                                                                                                                                                        Object load_obj = dataMap.get("load");
                                                                                                                                                                                                        if(load_obj != null){
                                                                                                                                                                                                            load = convertObjectToBigDecimal(load_obj);
                                                                                                                                                                                                        }
                                                                                                                                                                                                        Object solartotalinputacc_obj = dataMap.get("solartotalinputacc");
                                                                                                                                                                                                        if(solartotalinputacc_obj != null){
                                                                                                                                                                                                            solartotalinputacc = convertObjectToBigDecimal(solartotalinputacc_obj);
                                                                                                                                                                                                        }
                                                                                                                                                                                                        Object solartotaloutputacc_obj = dataMap.get("solartotaloutputacc");
                                                                                                                                                                                                        if(solartotaloutputacc_obj != null){
                                                                                                                                                                                                            solartotaloutputacc = convertObjectToBigDecimal(solartotaloutputacc_obj);
                                                                                                                                                                                                        }

                                                                                                                                                                                                        griduseall[0] = griduseall[0].add(griduse);
                                                                                                                                                                                                        loadall[0] = loadall[0].add(load);
                                                                                                                                                                                                        solartotalinputaccall[0] = solartotalinputaccall[0].add(solartotalinputacc);
                                                                                                                                                                                                        solartotaloutputaccall[0] = solartotaloutputaccall[0].add(solartotaloutputacc);


                                                                                                                                                                                                        isStartRef3s1g4[0] = false;
                                                                                                                                                                                                        // ParameterRealtime3S1G5
                                                                                                                                                                                                        ref3s1g5.addValueEventListener(new ValueEventListener() {
                                                                                                                                                                                                            @Override
                                                                                                                                                                                                            public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                                                                                                                if(dataSnapshot.exists() && isStartRef3s1g5[0]){
                                                                                                                                                                                                                    HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                                                                                                    Object obj = hashMapData.get("DataRealtime3S1G5");
                                                                                                                                                                                                                    if (obj == null) {
                                                                                                                                                                                                                        return;
                                                                                                                                                                                                                    }
                                                                                                                                                                                                                    HashMap dataMap = (HashMap) obj;
                                                                                                                                                                                                                    BigDecimal griduse = BigDecimal.ZERO;
                                                                                                                                                                                                                    BigDecimal load = BigDecimal.ZERO;
                                                                                                                                                                                                                    BigDecimal solartotalinputacc = BigDecimal.ZERO;
                                                                                                                                                                                                                    BigDecimal solartotaloutputacc = BigDecimal.ZERO;
                                                                                                                                                                                                                    Object griduse_obj = dataMap.get("griduse");
                                                                                                                                                                                                                    if(griduse_obj != null){
                                                                                                                                                                                                                        griduse = convertObjectToBigDecimal(griduse_obj);
                                                                                                                                                                                                                    }
                                                                                                                                                                                                                    Object load_obj = dataMap.get("load");
                                                                                                                                                                                                                    if(load_obj != null){
                                                                                                                                                                                                                        load = convertObjectToBigDecimal(load_obj);
                                                                                                                                                                                                                    }
                                                                                                                                                                                                                    Object solartotalinputacc_obj = dataMap.get("solartotalinputacc");
                                                                                                                                                                                                                    if(solartotalinputacc_obj != null){
                                                                                                                                                                                                                        solartotalinputacc = convertObjectToBigDecimal(solartotalinputacc_obj);
                                                                                                                                                                                                                    }
                                                                                                                                                                                                                    Object solartotaloutputacc_obj = dataMap.get("solartotaloutputacc");
                                                                                                                                                                                                                    if(solartotaloutputacc_obj != null){
                                                                                                                                                                                                                        solartotaloutputacc = convertObjectToBigDecimal(solartotaloutputacc_obj);
                                                                                                                                                                                                                    }

                                                                                                                                                                                                                    griduseall[0] = griduseall[0].add(griduse);
                                                                                                                                                                                                                    loadall[0] = loadall[0].add(load);
                                                                                                                                                                                                                    solartotalinputaccall[0] = solartotalinputaccall[0].add(solartotalinputacc);
                                                                                                                                                                                                                    solartotaloutputaccall[0] = solartotaloutputaccall[0].add(solartotaloutputacc);


                                                                                                                                                                                                                    isStartRef3s1g5[0] = false;
                                                                                                                                                                                                                    // ParameterRealtime3S1G6
                                                                                                                                                                                                                    ref3s1g6.addValueEventListener(new ValueEventListener() {
                                                                                                                                                                                                                        @Override
                                                                                                                                                                                                                        public void onDataChange(DataSnapshot dataSnapshot) {
                                                                                                                                                                                                                            if(dataSnapshot.exists() && isStartRef3s1g6[0]){
                                                                                                                                                                                                                                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                                                                                                                                                                                                                                Object obj = hashMapData.get("DataRealtime3S1G6");
                                                                                                                                                                                                                                if (obj == null) {
                                                                                                                                                                                                                                    return;
                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                HashMap dataMap = (HashMap) obj;
                                                                                                                                                                                                                                BigDecimal griduse = BigDecimal.ZERO;
                                                                                                                                                                                                                                BigDecimal load = BigDecimal.ZERO;
                                                                                                                                                                                                                                BigDecimal solartotalinputacc = BigDecimal.ZERO;
                                                                                                                                                                                                                                BigDecimal solartotaloutputacc = BigDecimal.ZERO;
                                                                                                                                                                                                                                Object griduse_obj = dataMap.get("griduse");
                                                                                                                                                                                                                                if(griduse_obj != null){
                                                                                                                                                                                                                                    griduse = convertObjectToBigDecimal(griduse_obj);
                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                Object load_obj = dataMap.get("load");
                                                                                                                                                                                                                                if(load_obj != null){
                                                                                                                                                                                                                                    load = convertObjectToBigDecimal(load_obj);
                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                Object solartotalinputacc_obj = dataMap.get("solartotalinputacc");
                                                                                                                                                                                                                                if(solartotalinputacc_obj != null){
                                                                                                                                                                                                                                    solartotalinputacc = convertObjectToBigDecimal(solartotalinputacc_obj);
                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                Object solartotaloutputacc_obj = dataMap.get("solartotaloutputacc");
                                                                                                                                                                                                                                if(solartotaloutputacc_obj != null){
                                                                                                                                                                                                                                    solartotaloutputacc = convertObjectToBigDecimal(solartotaloutputacc_obj);
                                                                                                                                                                                                                                }

                                                                                                                                                                                                                                griduseall[0] = griduseall[0].add(griduse);
                                                                                                                                                                                                                                loadall[0] = loadall[0].add(load);
                                                                                                                                                                                                                                solartotalinputaccall[0] = solartotalinputaccall[0].add(solartotalinputacc);
                                                                                                                                                                                                                                solartotaloutputaccall[0] = solartotaloutputaccall[0].add(solartotaloutputacc);

//                                                                                                                                                                                                                                log.info("gridkwTall : {}", gridkwTall[0]);
//                                                                                                                                                                                                                                log.info("LoadkwTall : {}", LoadkwTall[0]);
//                                                                                                                                                                                                                                log.info("solartotalinputall : {}", solartotalinputall[0]);
//                                                                                                                                                                                                                                log.info("solartotaloutputall : {}", solartotaloutputall[0]);
//                                                                                                                                                                                                                                log.info("persengridall : {}", persengridall[0]);
//                                                                                                                                                                                                                                log.info("persenpvall : {}", persenpvall[0]);
//                                                                                                                                                                                                                                log.info("persensolarall : {}", persensolarall[0]);
//                                                                                                                                                                                                                                log.info("griduseall : {}", griduseall[0]);
//                                                                                                                                                                                                                                log.info("loadall : {}", loadall[0]);
//                                                                                                                                                                                                                                log.info("solartotalinputaccall : {}", solartotalinputaccall[0]);
//                                                                                                                                                                                                                                log.info("solartotaloutputaccall : {}", solartotaloutputaccall[0]);

                                                                                                                                                                                                                                int decimalPlacesOne = 1;
                                                                                                                                                                                                                                int decimalPlacesZero = 0;

                                                                                                                                                                                                                                gridkwTall[0] = gridkwTall[0].setScale(decimalPlacesOne, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                LoadkwTall[0] = LoadkwTall[0].setScale(decimalPlacesOne, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                solartotalinputall[0] = solartotalinputall[0].setScale(decimalPlacesOne, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                solartotaloutputall[0] = solartotaloutputall[0].setScale(decimalPlacesOne, BigDecimal.ROUND_HALF_UP);

                                                                                                                                                                                                                                persengridall[0] = persengridall[0].setScale(decimalPlacesZero, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                persenpvall[0] = persenpvall[0].setScale(decimalPlacesZero, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                persensolarall[0] = persensolarall[0].setScale(decimalPlacesZero, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                griduseall[0] = griduseall[0].setScale(decimalPlacesZero, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                loadall[0] = loadall[0].setScale(decimalPlacesZero, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                solartotalinputaccall[0] = solartotalinputaccall[0].setScale(decimalPlacesZero, BigDecimal.ROUND_HALF_UP);
                                                                                                                                                                                                                                solartotaloutputaccall[0] = solartotaloutputaccall[0].setScale(decimalPlacesZero, BigDecimal.ROUND_HALF_UP);

                                                                                                                                                                                                                                userUpdates.put("gridkwTall", gridkwTall[0].toString());
                                                                                                                                                                                                                                userUpdates.put("LoadkwTall", LoadkwTall[0].toString());

                                                                                                                                                                                                                                userUpdates.put("solartotalinputall", solartotalinputall[0].toString());
                                                                                                                                                                                                                                userUpdates.put("solartotaloutputall", solartotaloutputall[0].toString());
                                                                                                                                                                                                                                userUpdates.put("persengridall", persengridall[0].toString());
                                                                                                                                                                                                                                userUpdates.put("persenpvall", persenpvall[0].toString());
                                                                                                                                                                                                                                userUpdates.put("persensolarall", persensolarall[0].toString());

                                                                                                                                                                                                                                userUpdates.put("griduseall", griduseall[0].toString());
                                                                                                                                                                                                                                userUpdates.put("loadall", loadall[0].toString());
                                                                                                                                                                                                                                userUpdates.put("solartotalinputaccall", solartotalinputaccall[0].toString());
                                                                                                                                                                                                                                userUpdates.put("solartotaloutputaccall", solartotaloutputaccall[0].toString());
                                                                                                                                                                                                                                log.info("userUpdates : {}", userUpdates);
                                                                                                                                                                                                                                try {
                                                                                                                                                                                                                                    refTotal.setValueAsync(userUpdates);
                                                                                                                                                                                                                                }catch (Exception e){
                                                                                                                                                                                                                                    e.printStackTrace();
                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                isStartRef3s1g6[0] = false;
                                                                                                                                                                                                                                log.info("End processQueueTotal at {}", new Date());

                                                                                                                                                                                                                            }
                                                                                                                                                                                                                        }

                                                                                                                                                                                                                        @Override
                                                                                                                                                                                                                        public void onCancelled(DatabaseError databaseError) {
                                                                                                                                                                                                                            log.error("The read failed: " + databaseError.getCode());
                                                                                                                                                                                                                        }
                                                                                                                                                                                                                    });


                                                                                                                                                                                                                }
                                                                                                                                                                                                            }

                                                                                                                                                                                                            @Override
                                                                                                                                                                                                            public void onCancelled(DatabaseError databaseError) {
                                                                                                                                                                                                                log.error("The read failed: " + databaseError.getCode());
                                                                                                                                                                                                            }
                                                                                                                                                                                                        });


                                                                                                                                                                                                    }
                                                                                                                                                                                                }

                                                                                                                                                                                                @Override
                                                                                                                                                                                                public void onCancelled(DatabaseError databaseError) {
                                                                                                                                                                                                    log.error("The read failed: " + databaseError.getCode());
                                                                                                                                                                                                }
                                                                                                                                                                                            });


                                                                                                                                                                                        }
                                                                                                                                                                                    }

                                                                                                                                                                                    @Override
                                                                                                                                                                                    public void onCancelled(DatabaseError databaseError) {
                                                                                                                                                                                        log.error("The read failed: " + databaseError.getCode());
                                                                                                                                                                                    }
                                                                                                                                                                                });


                                                                                                                                                                            }
                                                                                                                                                                        }

                                                                                                                                                                        @Override
                                                                                                                                                                        public void onCancelled(DatabaseError databaseError) {
                                                                                                                                                                            log.error("The read failed: " + databaseError.getCode());
                                                                                                                                                                        }
                                                                                                                                                                    });


                                                                                                                                                                }
                                                                                                                                                            }

                                                                                                                                                            @Override
                                                                                                                                                            public void onCancelled(DatabaseError databaseError) {
                                                                                                                                                                log.error("The read failed: " + databaseError.getCode());
                                                                                                                                                            }
                                                                                                                                                        });


                                                                                                                                                    }
                                                                                                                                                }

                                                                                                                                                @Override
                                                                                                                                                public void onCancelled(DatabaseError databaseError) {
                                                                                                                                                    log.error("The read failed: " + databaseError.getCode());
                                                                                                                                                }
                                                                                                                                            });


                                                                                                                                        }
                                                                                                                                    }

                                                                                                                                    @Override
                                                                                                                                    public void onCancelled(DatabaseError databaseError) {
                                                                                                                                        log.error("The read failed: " + databaseError.getCode());
                                                                                                                                    }
                                                                                                                                });


                                                                                                                            }
                                                                                                                        }

                                                                                                                        @Override
                                                                                                                        public void onCancelled(DatabaseError databaseError) {
                                                                                                                            log.error("The read failed: " + databaseError.getCode());
                                                                                                                        }
                                                                                                                    });


                                                                                                                }
                                                                                                            }

                                                                                                            @Override
                                                                                                            public void onCancelled(DatabaseError databaseError) {
                                                                                                                log.error("The read failed: " + databaseError.getCode());
                                                                                                            }
                                                                                                        });



                                                                                                    }
                                                                                                }

                                                                                                @Override
                                                                                                public void onCancelled(DatabaseError databaseError) {
                                                                                                    log.error("The read failed: " + databaseError.getCode());
                                                                                                }
                                                                                            });


                                                                                        }
                                                                                    }

                                                                                    @Override
                                                                                    public void onCancelled(DatabaseError databaseError) {
                                                                                        log.error("The read failed: " + databaseError.getCode());
                                                                                    }
                                                                                });


                                                                            }
                                                                        }

                                                                        @Override
                                                                        public void onCancelled(DatabaseError databaseError) {
                                                                            log.error("The read failed: " + databaseError.getCode());
                                                                        }
                                                                    });



                                                                }
                                                            }

                                                            @Override
                                                            public void onCancelled(DatabaseError databaseError) {
                                                                log.error("The read failed: " + databaseError.getCode());
                                                            }
                                                        });


                                                    }
                                                }

                                                @Override
                                                public void onCancelled(DatabaseError databaseError) {
                                                    log.error("The read failed: " + databaseError.getCode());
                                                }
                                            });


                                        }
                                    }

                                    @Override
                                    public void onCancelled(DatabaseError databaseError) {
                                        log.error("The read failed: " + databaseError.getCode());
                                    }
                                });


                            }
                        }

                        @Override
                        public void onCancelled(DatabaseError databaseError) {
                            log.error("The read failed: " + databaseError.getCode());
                        }
                    });

                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                log.error("The read failed: " + databaseError.getCode());
            }
        });


}catch (Exception e){
    log.error("Exception : ",e);
    e.printStackTrace();
}finally {
    return userUpdates;
}

    }



    public void processQueue() {
        log.info("Start consume queue at {}", new Date());

        // Get a reference to our posts
        final FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference ref = database.getReference("ParameterDaily");
        DatabaseReference refTotal = database.getReference("ParameterDailyTotal");

        // Attach a listener to read the data at our posts reference
        ref.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
//                log.info("dataSnapshot : {}",dataSnapshot);
                if(dataSnapshot.exists()){
                    HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                    if(hashMapData != null) {
                        Long chartdailygrid_total = 0L;
                        Long chartdailyinv1pv_total = 0L;
                        Long chartdailyinv1solar_total = 0L;
                        Long chartdailyload_total = 0L;
                        Long chartdailypv_total = 0L;
                        Long chartdailysolar_total = 0L;
                        for (Map.Entry<String, HashMap> entry : hashMapData.entrySet()) {
                            HashMap parameterDailyMap = entry.getValue();
                            Object object = parameterDailyMap.get("date");
                            if (object == null) {
                                continue;
                            }

                            Object obj = parameterDailyMap.get("DataDaily");
                            if (obj == null) {
                                continue;
                            }
                            HashMap dataMap = (HashMap) obj;
                            Long chartdailygrid = 0L;
                            Long chartdailyinv1pv = 0L;
                            Long chartdailyinv1solar = 0L;
                            Long chartdailyload = 0L;
                            Long chartdailypv = 0L;
                            Long chartdailysolar = 0L;
                            Object chartdailygrid_obj = dataMap.get("chartdailygrid");
                            if(chartdailygrid_obj != null){
                                chartdailygrid = (Long) chartdailygrid_obj;
                            }
                            Object chartdailyinv1pv_obj = dataMap.get("chartdailyinv1pv");
                            if(chartdailyinv1pv_obj != null){
                                chartdailyinv1pv = (Long) chartdailyinv1pv_obj;
                            }
                            Object chartdailyinv1solar_obj = dataMap.get("chartdailyinv1solar");
                            if(chartdailyinv1solar_obj != null){
                                chartdailyinv1solar = (Long) chartdailyinv1solar_obj;
                            }
                            Object chartdailyload_obj = dataMap.get("chartdailyload");
                            if(chartdailyload_obj != null){
                                chartdailyload = (Long) chartdailyload_obj;
                            }
                            Object chartdailypv_obj = dataMap.get("chartdailypv");
                            if(chartdailypv_obj != null){
                                chartdailypv = (Long) chartdailypv_obj;
                            }
                            Object chartdailysolar_obj = dataMap.get("chartdailysolar");
                            if(chartdailysolar_obj != null){
                                chartdailysolar = (Long) chartdailysolar_obj;
                            }
                            chartdailygrid_total += chartdailygrid;
                            chartdailyinv1pv_total += chartdailyinv1pv;
                            chartdailyinv1solar_total += chartdailyinv1solar;
                            chartdailyload_total += chartdailyload;
                            chartdailypv_total += chartdailypv;
                            chartdailysolar_total += chartdailysolar;
                        }
                        log.info("chartdailygrid_total : {}",chartdailygrid_total);
                        log.info("chartdailyinv1pv_total : {}",chartdailyinv1pv_total);
                        log.info("chartdailyinv1solar_total : {}",chartdailyinv1solar_total);
                        log.info("chartdailyload_total : {}",chartdailyload_total);
                        log.info("chartdailypv_total : {}",chartdailypv_total);
                        log.info("chartdailysolar_total : {}",chartdailysolar_total);

                        Map<String, Object> userUpdates = new HashMap<>();
                        userUpdates.put("chartdailygrid_total", chartdailygrid_total);
                        userUpdates.put("chartdailyinv1pv_total", chartdailyinv1pv_total);
                        userUpdates.put("chartdailyinv1solar_total", chartdailyinv1solar_total);
                        userUpdates.put("chartdailyload_total", chartdailyload_total);
                        userUpdates.put("chartdailypv_total", chartdailypv_total);
                        userUpdates.put("chartdailysolar_total", chartdailysolar_total);
                        refTotal.setValueAsync(userUpdates);

                    }
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                log.error("The read failed: " + databaseError.getCode());
            }
        });

    }

    public void processQueueWeather() {
        final FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference weatherForecast7Days = database.getReference("WeatherForecast7Days");
        log.info("Start processQueueWeather at {}", new Date());
        final String uri = "https://data.tmd.go.th/api/WeatherForecast7Days/V1/?type=json";

        RestTemplate restTemplate = new RestTemplate();
        WeatherForecast7Days result = restTemplate.getForObject(uri, WeatherForecast7Days.class);
        log.info("WeatherForecast7Days : {}",result);
        weatherForecast7Days.setValueAsync(result);
    }

    public void processQueueWeatherToday() {
        final FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference WeatherTodayRef = database.getReference("WeatherToday");
        log.info("Start processQueueWeather at {}", new Date());
        final String uri = "https://data.tmd.go.th/api/WeatherToday/V1/?type=json";

        RestTemplate restTemplate = new RestTemplate();
        Object result = restTemplate.getForObject(uri, Object.class);
        log.info("WeatherToday : {}",result);
        WeatherTodayRef.setValueAsync(result);
    }

    public void processQueueWeather3Hours() {
        final FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference WeatherTodayRef = database.getReference("Weather3Hours");
        log.info("Start processQueueWeather at {}", new Date());
        final String uri = "https://data.tmd.go.th/api/Weather3Hours/V1/?type=json";

        RestTemplate restTemplate = new RestTemplate();
        Object result = restTemplate.getForObject(uri, Object.class);
        log.info("Weather3Hours : {}",result);
        WeatherTodayRef.setValueAsync(result);
    }
}
