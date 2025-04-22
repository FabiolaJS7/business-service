# Usa una imagen base con soporte para Java 17
FROM eclipse-temurin:17-jdk-alpine

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo JAR generado al contenedor
COPY target/business-service-0.0.1-SNAPSHOT.jar app.jar

# Expone el puerto en el que tu aplicación escucha (por ejemplo, 8080)
EXPOSE 8585

# Comando para ejecutar la aplicación
ENTRYPOINT ["java", "-jar", "app.jar"]