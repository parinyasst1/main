package th.co.solar.solarapi.service;

import com.google.firebase.database.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class RemoveDataService {

    // Get a reference to our posts
    FirebaseDatabase database = null;

    DatabaseReference namePath = null;
    List<String> namePathList = new ArrayList<>();

    final boolean[] isDelete = {true};

    public Map<String, Object> processQueue() {
        database = FirebaseDatabase.getInstance();
        namePath = database.getReference("NamePath");

        DatabaseReference parameterDailyG1 = database.getReference("ParameterDailyG1");
        DatabaseReference parameterDailyG2 = database.getReference("ParameterDailyG2");
        DatabaseReference parameterDailyG3 = database.getReference("ParameterDailyG3");
        DatabaseReference parameterDailyG4 = database.getReference("ParameterDailyG4");
        DatabaseReference parameterDailyG5 = database.getReference("ParameterDailyG5");
        DatabaseReference parameterDailyG6 = database.getReference("ParameterDailyG6");

        DatabaseReference ParameterMonthlyG1 = database.getReference("ParameterMonthlyG1");
        DatabaseReference ParameterMonthlyG2 = database.getReference("ParameterMonthlyG2");
        DatabaseReference ParameterMonthlyG3 = database.getReference("ParameterMonthlyG3");
        DatabaseReference ParameterMonthlyG4 = database.getReference("ParameterMonthlyG4");
        DatabaseReference ParameterMonthlyG5 = database.getReference("ParameterMonthlyG5");
        DatabaseReference ParameterMonthlyG6 = database.getReference("ParameterMonthlyG6");

        namePath.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                log.info("----- RemoveDataService -----");
                namePathList = (List<String>) dataSnapshot.getValue();
                if (isDelete[0]) {
                    log.info("namePathList : {}", namePathList);
                    removeDataParameterDaily("ParameterDailyG1", "G1");
                    removeDataParameterDaily("ParameterDailyG2", "G2");
                    removeDataParameterDaily("ParameterDailyG3", "G3");
                    removeDataParameterDaily("ParameterDailyG4", "G4");
                    removeDataParameterDaily("ParameterDailyG5", "G5");
                    removeDataParameterDaily("ParameterDailyG6", "G6");

                    removeDataParameterMonthly("ParameterMonthlyG1", "G1");
                    removeDataParameterMonthly("ParameterMonthlyG2", "G2");
                    removeDataParameterMonthly("ParameterMonthlyG3", "G3");
                    removeDataParameterMonthly("ParameterMonthlyG4", "G4");
                    removeDataParameterMonthly("ParameterMonthlyG5", "G5");
                    removeDataParameterMonthly("ParameterMonthlyG6", "G6");
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });

        return null;
    }

    public void removeDataParameterMonthly(String nameRef,String group){
        DatabaseReference databaseReference = database.getReference(nameRef);
        databaseReference.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                LocalDateTime now = LocalDateTime.now().minusMonths(6);
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String name:namePathList){
                    Object obj = hashMapData.get(name+group);
                    if (obj != null) {
                        HashMap<String,HashMap> data = (HashMap<String,HashMap>) obj;
                        data.forEach((key, hashMap) -> {
                            String dateStr = (String) hashMap.get("date");
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
                            LocalDateTime date = LocalDateTime.parse(dateStr, formatter);
//                            log.info("now : {} = date : {}",now,date);
//                            log.info("{}",now.compareTo(date));
                            if(now.compareTo(date) > 0){
                                log.info("removeDataParameterMonthly : {}",key);
                                removeData(nameRef,name+group,key);
                            }
                        });

                    }
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                log.error("onCancelled : {}", databaseError.toString());
            }
        });
    }

    public void removeDataParameterDaily(String nameRef,String group){
        DatabaseReference databaseReference = database.getReference(nameRef);
        databaseReference.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                LocalDateTime now = LocalDateTime.now().minusDays(8);
                HashMap<String,HashMap> hashMapData = (HashMap<String,HashMap>) dataSnapshot.getValue();
                for(String name:namePathList){
                    Object obj = hashMapData.get(name+group);
                    if (obj != null) {
                        HashMap<String,HashMap> data = (HashMap<String,HashMap>) obj;
                        data.forEach((key, hashMap) -> {
                            String dateStr = (String) hashMap.get("date");
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
                            LocalDateTime date = LocalDateTime.parse(dateStr, formatter);
//                            log.info("now : {} = date : {}",now,date);
//                            log.info("{}",now.compareTo(date));
                            if(now.compareTo(date) > 0){
                                log.info("removeDataParameterDaily : {}",key);
                                removeData(nameRef,name+group,key);
                            }
                        });

                    }
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                log.error("onCancelled : {}", databaseError.toString());
            }
        });
    }

    public void removeData(String name,String group,String key){
        DatabaseReference ref = FirebaseDatabase.getInstance().getReference();
        Query applesQuery = ref.child(name+"/"+group+"/"+key);
        applesQuery.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                for (DataSnapshot appleSnapshot: dataSnapshot.getChildren()) {
                    appleSnapshot.getRef().removeValueAsync();
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                log.error("onCancelled : {}", databaseError.toString());
            }
        });
    }
}
