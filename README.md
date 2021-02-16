# schoolsfinder


#### Generating Uber Jar

`sbt  assembly` will generate 'schoolsfinder-assembly-0.1.jar' under 'target/scala-2.12' folder


`application.conf` provides the config details for the Application. 

```
schoolsfinder {
       master = "local"
       appName = "SchoolsFinderApp"
       schoolsdata {
           school_information = "data/england_school_information.csv"
           school_inspection = "data/schools_inspections_2021.csv"

           dataFormat = "csv"
           separator = ","
       }
       ethnicsdata {
            constituency_ethnics_data = "data/constituency_ethnics_data.csv"
       }
}
```

#### Running Application

Spark submit command :  

`spark-submit target/scala-2.12/schoolsfinder-assembly-0.1.jar --files application.conf --class org.schools.SchoolsFinderApp`
  