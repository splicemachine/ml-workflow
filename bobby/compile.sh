
#!/bin/bash -f

sm_version=${sm_version:-"2.7.0.1828"}
airlift_version=${airlift_version:-"0.144"} 
smjars="db-client db-shared pipeline_api splice_protocol utilities db-drda db-tools-i18n splice_access_api splice_machine splice_si_api db-engine db-tools-ij splice_encoding splicemachine-cdh5.8.3-2.1.1_2.11 splice_timestamp_api"

#mvn clean compile package -Pcdh5.8.3

if [ $? -eq 0 ]; then
    if [ -d ./target/splicemachine-jars ]; then rm -rf ./target/splicemachine-jars; fi
    mkdir ./target/splicemachine-jars
    for smjar in $smjars; do
        cp ~/.m2/repository/com/splicemachine/$smjar/$sm_version/$smjar-$sm_version.jar ./target/splicemachine-jars/
    done
    cp ~/.m2/repository/com/splicemachine/splice_aws/$sm_version/splice_aws-$sm_version-shade.jar ./target/splicemachine-jars/
    
    cp ~/.m2/repository/io/airlift/log/$airlift_version/log-$airlift_version.jar ./target/splicemachine-jars/
    cp ~/.m2/repository/io/airlift/stats/$airlift_version/stats-$airlift_version.jar ./target/splicemachine-jars/
    cp ~/.m2/repository/io/airlift/slice/0.28/slice-0.28.jar ./target/splicemachine-jars/
    cp ~/.m2/repository/org/openjdk/jol/jol-core/0.2/jol-core-0.2.jar ./target/splicemachine-jars/
    cp ~/.m2/repository/io/airlift/units/1.0/units-1.0.jar ./target/splicemachine-jars/
                  
fi
