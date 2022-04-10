# Data Engineer Evaluation 1
### Introducción

- Este proyecto fue construído en la Nube de Amazon utilizando las siguientes herramientas: 
  * AWS Aurora MySql
  * AWS Glue Studio
  * AWS SageMaker
  * AWS QuickSight
- Este repositorio hace referencia al código generado en dichas herramientas. 
- No contiene un script de ejecución.
- El mantenimiento de dicho código se lo debe realizar en las mismas herramientas.

### Contenido

- Este repositorio contiene los siguientes folders:
  * aws_glue:  ETL's generados en aws_glue
    * src_average_salaries 
    * src_hall_of_fame_all_star_pitchers 
    * src_pitching 
    * src_rankings
  * aws_s3: notebooks utilizados para interactuar con los resultados.
    * Nodel_s3_to_csv.ipynb:  Contiene el código para leer de S3 los archivos generados de los ETL's y transformarlos a csv.
  * output: contiene los archivos resultado de los procesos ETL.
    * ex_1: Average Salaries  
    * ex_2: Hall of Fame All Star Pitchers
    * ex_3: Pitching
    * ex_4: Rankings


### Presentation
- En el siguiente link encontrará la presentación de este proyecto:
  * https://docs.google.com/presentation/d/1iteX7ojJ4yMGjC8q9fIpek2LUv8mjf_z1h3KYDcDlaQ/edit?usp=sharing

### Plots
- Los gráficos realizados en QuickSight podrán  ser visualizados en la misma herramienta o en la presentación descrita en el punto anterior.
