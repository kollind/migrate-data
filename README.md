# migrate-data
Migrate data from SQL to Postgre

connectors - содержат логику подключения и выполнения скриптов

utils - вспомогательные функции для логгирования выполнения программы и сохранения состояний миграции

main - основной код программы, который реализует миграцию

Если миграция была остановлена или упала - продолжение копирования данных начнется со следующего, после упавшего, батча данных

.env.example содержит структуру конфигурационного файла. 
