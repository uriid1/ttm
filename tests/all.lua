local ttm = require('ttm')

box.cfg {}

ttm.yield_every = 100

ttm.init()
ttm.run()

local function p(text, value)
  if value ~= nil then
    io.write(
      "\27[38;5;226m", text, "\27[0m",
      " ", "\27[38;5;13m", tostring(value), "\27[0m", '\n')
  else
    io.write("\27[38;5;226m", text, "\27[0m", '\n')
  end
  io.flush()
end

do -- Тест: Выполнение разовых действий по заданному ttl
  local value = 100200300
  local ttl = 5 -- Секунд

  if ttm.try_once('one_time_action', value, ttl) then
    p("Действие разрешено")
  else
    p("Слишком частые запросы")
  end
end

do -- Тест: Привязка временных данных
  local userId = 1234567890
  local ttl = 30
  local primaryRec = "users"
  local key = "click_reload_command"

  -- Привязка к primaryRec ключа со значением и заданным ttl
  ttm.add(primaryRec, key, userId, ttl)

  -- Проверка существования первичных данных
  local value = ttm.get(primaryRec, key)

  p("Значение:", value)
  p("Осталось времени:", ttm.time_left(primaryRec, key))
end

do -- Статистика
  local stats = ttm.get_stats()

  p("Всего записей:", stats.total)
  p("Активных:", stats.active)
end
