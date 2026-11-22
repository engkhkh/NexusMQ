# NexusMQ broker image.
#   docker build -t nexusmq .
#   docker run -p 5680:5680 -v nexusmq-data:/data -e NexusMQ__ApiKey=change-me nexusmq
FROM mcr.microsoft.com/dotnet/sdk:11.0 AS build
WORKDIR /src
COPY Directory.Build.props README.md ./
COPY src/ src/
RUN dotnet publish src/NexusMQ.Server/NexusMQ.Server.csproj -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
RUN mkdir -p /data && chown $APP_UID /data
USER $APP_UID
ENV ASPNETCORE_HTTP_PORTS=5680 \
    NexusMQ__DataDirectory=/data
VOLUME /data
EXPOSE 5680
ENTRYPOINT ["dotnet", "NexusMQ.Server.dll"]
