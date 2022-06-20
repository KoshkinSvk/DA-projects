# DA-projects

- [ARPU metric](https://github.com/KoshkinSvk/DA-projects/tree/main/ARPU_analysis) -  Кейс метрики ARPU (average revenue per user) аудитории сайта за май 2019 года.
- [Credit scoring](https://github.com/KoshkinSvk/DA-projects/tree/main/Credit%20scoring) - Построение моделей кредитного скоринга клиентов банка, а также оценка точности предсказания.
- [Multilabels predictions](https://github.com/KoshkinSvk/DA-projects/tree/main/Multilabels%20prediction) - Построение моделей предсказания линейной зависимости с множественными лейблами, поиск наиболее важных признаков для предсказания.
- [Rental analysis](https://github.com/KoshkinSvk/DA-projects/tree/main/RE_analysis) - Анализ рынка недвижимости в Санкт-Петербурге и ЛО.
- [Taxi order prediction](https://github.com/KoshkinSvk/DA-projects/tree/main/Taxi%20order_prediction) - Построение моделей предсказания заказов такси и оценка точности предсказания.
- [A/A, A/B tests](https://github.com/KoshkinSvk/DA-projects/tree/main/AA%2C%20AB%20tests) - Пример проведения А/А теста с бутстрепом для проверки системы сплитования и А/В тестов для проверки гипотез об улучшении метрики CTR на группах.
- [report automatization](https://github.com/KoshkinSvk/DA-projects/tree/main/test%20reports) - Пример автоматизации аналитической сводки по необходимым метрикам через GitLab CI/CD. В указанный срок в чат оправлется сообщение со значениями требуемых метрик за предыдущий день и графики со значениями за 7 дней. 
- [metric alerts](https://github.com/KoshkinSvk/DA-projects/tree/main/metric%20alerts) - Пример системы алертов основновых метрик путем детектирования аномалий методом скользящих средних квантилей через GitLab CI/CD. В случае триггера в чат  отправляется алерт - сообщение с метрикой ее величиной и величиной отклонения, а также график с верхним и нижним пределом.
- [ETL pipeline](https://github.com/KoshkinSvk/DA-projects/blob/main/af_pipeline_to_ch.py) - Пример построения ETL пайплайна. Для каждого юзера считаем число просмотров и лайков, а также кол-во полученных и отолсанных сообщений. Сортруем по полу, возрасту и ос. Финальные данные записывает в отдельную таблицу clickhouse.
