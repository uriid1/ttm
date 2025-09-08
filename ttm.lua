--- Temp Table Manager
--
local log = require('log')
local fiber = require('fiber')
local clock = require('clock')

local ttm = {}

-- Параметры по умолчанию
ttm.interval = 60
ttm.yield_every = 100

-- Инициализация служебного спейса
function ttm.init()
  box.once('ttm_init', function()
    local space = box.schema.create_space('temp_data', {
      format = {
        { name = 't_key',   type = 'string' }, -- Имя логической записи
        { name = 'key',     type = 'string' }, -- Ключ (приводится к строке)
        { name = 'value',   type = 'any'    }, -- Произвольные данные
        { name = 'created', type = 'number' },
        { name = 'ttl',     type = 'number' }
      },
      if_not_exists = true
    })

    space:create_index('primary', {
      parts = { 't_key', 'key' }
    })

    space:create_index('by_expire', {
      parts = { 'created', 'ttl' },
      unique = false
    })
  end)
end

--- Добавление (или обновление) записи
-- @param t_key (string) - Имя логической записи
-- @param key (string) - Ключ записи
-- @param value (any) - Значение (по умолчанию true)
-- @param ttl (number) - TTL в секундах
-- @return boolean - true если добавлено впервые, false если запись ещё жива
function ttm.add(t_key, key, value, ttl)
  value = value or true
  ttl = ttl or 60
  key = tostring(key)

  local now = clock.time()
  local existing = box.space.temp_data:get({ t_key, key })

  if existing then
    local created, old_ttl = existing.created, existing.ttl
    if now - created < old_ttl then
      return false
    end
  end

  box.space.temp_data:put({ t_key, tostring(key), value, now, ttl })

  return true
end

--- Проверка/регистрация одноразового действия
-- @param t_key (string) - Имя логической записи
-- @param key (string) - Ключ записи
-- @param ttl (number) - TTL в секундах
-- @return boolean - true если разрешено, false если еще рано
function ttm.try_once(t_key, key, ttl)
  return ttm.add(t_key, key, true, ttl)
end

--- Возвращает оставшееся время до истечения записи
-- @param t_key (string) - Имя логической записи
-- @param key (string) - Ключ записи
-- @return number|nil - Секунды до истечения или nil если запись не найдена
function ttm.time_left(t_key, key)
  key = tostring(key)

  local rec = box.space.temp_data:get({ t_key, key })
  if not rec then
    return nil
  end

  local remaining = rec.ttl - (clock.time() - rec.created)

  return (remaining > 0) and remaining or 0
end

--- Проверка существования активной записи
-- @param t_key (string) - Имя логической записи
-- @param key (string) - Ключ записи
-- @return boolean - true если запись существует и не истекла
function ttm.exists(t_key, key)
  local time_left = ttm.time_left(t_key, key)
  return time_left ~= nil and time_left > 0
end

--- Проверка значения записи, если она активна
-- @param t_key string - Имя логической записи
-- @param key string - Ключ записи
-- @return any|nil - Значение записи или nil если не найдена/истекла
function ttm.get(t_key, key)
  key = tostring(key)

  local rec = box.space.temp_data:get({ t_key, key })
  if not rec then
    return nil
  end

  -- Проверка на истечение
  local now = clock.time()
  if now - rec.created >= rec.ttl then
    return nil
  end

  return rec.value
end

--- Удаление записи
-- @param t_key (string) - Имя логической записи
-- @param key (string) - Ключ записи
-- @return boolean - true если запись была удалена
function ttm.delete(t_key, key)
  key = tostring(key)

  local existing = box.space.temp_data:get({ t_key, key })

  if existing then
    box.space.temp_data:delete({ t_key, key })
    return true
  end

  return false
end

--- Очистка устаревших записей
function ttm:sweep()
  if not box.space.temp_data then
    return
  end

  local now = clock.time()
  local expired = {}
  local yield_every = self.yield_every
  local processed = 0

  -- Сборка истекших записей
  -- Проход по индексу by_expire для оптимизации
  for _, tuple in box.space.temp_data.index.by_expire:pairs() do
    local t_key = tuple[1]
    local key = tuple[2]
    local created = tuple[4]
    local ttl  = tuple[5]

    if now - created >= ttl then
      table.insert(expired, { t_key, key })
    else
      -- Поскольку индекс отсортирован по времени создания,
      -- все последующие записи будут более новыми
      break
    end

    processed = processed + 1
    if processed % yield_every == 0 then
      fiber.yield()
    end
  end

  -- Удаляем истекшие записи
  for _, record in ipairs(expired) do
    box.space.temp_data:delete({ record[1], record[2] })

    processed = processed + 1
    if processed % yield_every == 0 then
      fiber.yield()
    end
  end

  if #expired > 0 then
    log.info(string.format("TTM: Cleaned %d expired records", #expired))
  end
end

--- Получение статистики
-- @return table - общая статистика и по логическим таблицам
function ttm.get_stats()
  if not box.space.temp_data then
    return {
      total = 0,
      active = 0,
      expired = 0,
      by_table = {}
    }
  end

  local stats = {
    total = 0,
    active = 0,
    expired = 0,
    by_table = {}
  }
  local now = clock.time()

  for _, tuple in box.space.temp_data:pairs() do
    local t_key = tuple[1]
    local created = tuple[4]
    local ttl  = tuple[5]

    stats.total = stats.total + 1

    if not stats.by_table[t_key] then
      stats.by_table[t_key] = { total = 0, active = 0, expired = 0 }
    end
    stats.by_table[t_key].total = stats.by_table[t_key].total + 1

    if now - created < ttl then
      stats.active = stats.active + 1
      stats.by_table[t_key].active = stats.by_table[t_key].active + 1
    else
      stats.expired = stats.expired + 1
      stats.by_table[t_key].expired = stats.by_table[t_key].expired + 1
    end
  end

  return stats
end

-- Запуск фонового fiber-а для очистки
function ttm.run()
  fiber.create(function()
    fiber.self():name('ttm_sweeper')

    while true do
      fiber.sleep(ttm.interval)
      ttm:sweep()
    end
  end)
end

return ttm
