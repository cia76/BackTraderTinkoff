from typing import Union  # Объединение типов
import collections
from uuid import uuid4  # Номера заявок должны быть уникальными во времени и пространстве
from threading import Thread
import logging

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

from BackTraderTinkoff import TKStore

from TinkoffPy import TinkoffPy
from TinkoffPy.grpc.operations_pb2 import PortfolioRequest, PortfolioResponse  # Портфель
from TinkoffPy.grpc.orders_pb2 import (
    PostOrderRequest, PostOrderResponse, CancelOrderRequest,
    ORDER_DIRECTION_BUY, ORDER_DIRECTION_SELL, ORDER_TYPE_MARKET, ORDER_TYPE_LIMIT, OrderTrades)  # Заявка
from TinkoffPy.grpc.stoporders_pb2 import (
    PostStopOrderRequest, PostStopOrderResponse, CancelStopOrderRequest,
    STOP_ORDER_DIRECTION_BUY, STOP_ORDER_DIRECTION_SELL, StopOrderExpirationType, StopOrderType)  # Стоп-заявка


# noinspection PyArgumentList
class MetaTKBroker(BrokerBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaTKBroker, self).__init__(name, bases, dct)  # Инициализируем класс брокера
        TKStore.BrokerCls = self  # Регистрируем класс брокера в хранилище Tinkoff


# noinspection PyProtectedMember,PyArgumentList,PyUnusedLocal
class TKBroker(with_metaclass(MetaTKBroker, BrokerBase)):
    """Брокер Tinkoff"""
    params = (
        ('provider_name', None),  # Название провайдера. Если не задано, то первое название по ключу name
        ('use_positions', True),  # При запуске брокера подтягиваются текущие позиции с биржи
    )
    currency = PortfolioRequest.CurrencyRequest.RUB  # Суммы будем получать в российских рублях

    def __init__(self, **kwargs):
        super(TKBroker, self).__init__()
        self.store = TKStore(**kwargs)  # Хранилище Tinkoff
        self.provider_name = self.p.provider_name if self.p.provider_name else list(self.store.providers.keys())[0]  # Название провайдера, или первое название по ключу name
        self.logger = logging.getLogger(f'FNBroker.{self.provider_name}')  # Будем вести лог
        self.provider: TinkoffPy = self.store.providers[self.provider_name][0]  # Провайдер
        self.account_id = self.store.providers[self.provider_name][1]  # Счет
        self.logger.debug(f'Торговый счет {self.account_id}')
        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках
        self.startingcash = self.cash = self.getcash()  # Стартовые и текущие свободные средства по счету
        self.startingvalue = self.value = self.getvalue()  # Стартовая и текущая стоимость позиций
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.ocos = {}  # Список связанных заявок (One Cancel Others)
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)

        self.provider.on_order_trades = self.on_order_trades  # Обработка сделок по заявке
        Thread(target=self.provider.subscriptions_trades_handler, name='SubscriptionsTradesThread', args=(self.account_id,)).start()  # Создаем и запускаем поток обработки подписок сделок по заявке

    def start(self):
        super(TKBroker, self).start()
        if self.p.use_positions:  # Если нужно при запуске брокера получить текущие позиции на бирже
            self.get_all_active_positions()  # то получаем их

    def getcash(self):
        """Свободные средства по счету"""
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            request = PortfolioRequest(account_id=self.account_id, currency=self.currency)  # Запрос портфеля по счету в рублях
            response: PortfolioResponse = self.provider.call_function(self.provider.stub_operations.GetPortfolio, request)  # Портфель по счету
            self.cash = self.provider.money_value_to_float(response.total_amount_currencies)  # Свободные средства по счету
        return self.cash

    def getvalue(self, datas=None):
        """Стоимость позиции, позиций, всех позиций"""
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            value = 0  # Будем набирать стоимость позиций
            request = PortfolioRequest(account_id=self.account_id, currency=self.currency)  # Запрос портфеля по счету в рублях
            response: PortfolioResponse = self.provider.call_function(self.provider.stub_operations.GetPortfolio, request)  # Портфель по счету
            if datas is not None:  # Если получаем по тикерам
                for data in datas:  # Пробегаемся по всем тикерам
                    class_code, symbol = self.provider.dataname_to_class_code_symbol(data._name)  # По тикеру получаем площадку и код тикера
                    si = self.provider.get_symbol_info(class_code, symbol)  # Поиск тикера по коду площадки/названию
                    position = next((item for item in response.positions if item.figi == si.figi), None)  # Пытаемся получить позицию по уникальному коду тикера
                    if not position:  # Если позиция не найдена
                        continue  # то переходим к следующему тикеру
                    value += self.provider.money_value_to_float(position.current_price) * self.provider.quotation_to_float(position.quantity)  # Текущая ст-ть * Размер позиции
            else:  # Если получаем по счету
                value = self.provider.money_value_to_float(response.total_amount_portfolio)  # Оценка портфеля
                value -= self.provider.money_value_to_float(response.total_amount_currencies)  # без свободных средств по счету
            self.value = value  # Стоимость позиций
        return self.value

    def getposition(self, data):
        """Позиция по тикеру
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.positions[data._name]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, True, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера о принятии/отклонении зявки на бирже
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, False, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера о принятии/отклонении зявки на бирже
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.cancel_order(order)

    def get_notification(self):
        return self.notifs.popleft() if self.notifs else None  # Удаляем и возвращаем крайний левый элемент списка уведомлений или ничего

    def next(self):
        self.notifs.append(None)  # Добавляем в список уведомлений пустой элемент

    def stop(self):
        super(TKBroker, self).stop()
        self.provider.on_order_trades = self.provider.default_handler  # Обработка сделок по заявке
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Функции

    def get_all_active_positions(self):
        """Все активные позиции по счету"""
        request = PortfolioRequest(account_id=self.account_id, currency=self.currency)  # Запрос портфеля по счету в рублях
        response: PortfolioResponse = self.provider.call_function(self.provider.stub_operations.GetPortfolio, request)  # Портфель по счету
        for position in response.positions:  # Пробегаемся по всем активным позициям счета
            si = self.provider.figi_to_symbol_info(position.figi)  # Поиск тикера по уникальному коду
            dataname = self.provider.class_code_symbol_to_dataname(si.instrument.class_code, si.instrument.ticker)
            self.positions[dataname] = Position(self.provider.quotation_to_float(position.quantity), self.provider.money_value_to_float(position.average_position_price))  # Сохраняем в списке открытых позиций

    def get_order(self, order_id: str) -> Union[Order, None]:
        """Заявка BackTrader по номеру заявки на бирже
        Пробегаемся по всем заявкам на бирже. Если нашли совпадение с номером заявки на бирже, то возвращаем заявку BackTrader. Иначе, ничего не найдено

        :param str order_id: Номер заявки на бирже
        :return: Заявка BackTrader или None
        """
        return next((order for order in self.orders.values() if order.info['order_id'] == order_id), None)

    def create_order(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs):
        """Создание заявки. Привязка параметров счета и тикера. Обработка связанных и родительской/дочерних заявок
        Даполнительные параметры передаются через **kwargs:
        - portfolio - Портфель для площадки. Если не задан, то берется из Config.Boards
        - server - Торговый сервер для стоп заявок. Если не задан, то берется из Config.Boards
        """
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit)  # Заявка на покупку/продажу
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные свойства из брокера
        class_code, symbol = self.provider.dataname_to_class_code_symbol(data._name)  # По тикеру получаем код площадки и тикер
        order.addinfo(class_code=class_code, symbol=symbol)  # В заявку заносим код площадки class_code и тикер symbol
        if order.exectype in (Order.Close, Order.StopTrail, Order.StopTrailLimit, Order.Historical):  # Эти типы заявок не реализованы
            print(f'Постановка заявки {order.ref} по тикеру {class_code}.{symbol} отклонена. Работа с заявками {order.exectype} не реализована')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        si = self.provider.get_symbol_info(class_code, symbol)  # Поиск тикера по коду площадки/названию
        if not si:  # Если тикер не найден
            print(f'Постановка заявки {order.ref} по тикеру {class_code}.{symbol} отклонена. Тикер не найден')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        min_step = si.min_price_increment.units + si.min_price_increment.nano / 1_000_000_000  # Шаг цены
        if order.price:  # Если указана цена заявки
            order.price = order.price // min_step * min_step  # то ставим ее кратной шагу цены
        if order.pricelimit:  # Если указана лимитная цена
            order.pricelimit = order.pricelimit // min_step * min_step  # то ставим ее кратной шагу цены
        if order.exectype != Order.Market and not order.price:  # Если цена заявки не указана для всех заявок, кроме рыночной
            price_type = 'Лимитная' if order.exectype == Order.Limit else 'Стоп'  # Для стоп заявок это будет триггерная (стоп) цена
            print(f'Постановка заявки {order.ref} по тикеру {class_code}.{symbol} отклонена. {price_type} цена (price) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if order.exectype == Order.StopLimit and not order.pricelimit:  # Если лимитная цена не указана для стоп-лимитной заявки
            print(f'Постановка заявки {order.ref} по тикеру {class_code}.{symbol} отклонена. Лимитная цена (pricelimit) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                print(f'Постановка заявки {order.ref} по тикеру {class_code}.{symbol} отклонена. Родительская заявка не найдена')
                order.reject(self)  # то отклоняем заявку
                self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
                return order  # Возвращаем отклоненную заявку
            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)
        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            if not parent:  # Для обычных заявок
                return self.place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                self.notifs.append(order.clone())  # Удедомляем брокера о создании новой заявки
                return self.place_order(order.parent)  # Отправляем родительскую заявку на биржу
        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False)
        return order  # то возвращаем созданную заявку со статусом Created. На биржу ее пока не ставим

    def place_order(self, order: Order):
        """Отправка заявки на биржу"""
        class_code = order.info['class_code']  # Код площадки
        symbol = order.info['symbol']  # Код тикера
        si = self.provider.get_symbol_info(class_code, symbol)  # Поиск тикера по коду площадки/названию
        quantity: int = abs(order.size // si.lot)  # Размер позиции в лотах. В Тинькофф всегда передается положительный размер лота
        order_id = str(uuid4())  # Уникальный идентификатор заявки
        if order.exectype == Order.Market:  # Рыночная заявка
            direction = ORDER_DIRECTION_BUY if order.isbuy() else ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostOrderRequest(instrument_id=si.figi, quantity=quantity, direction=direction, account_id=self.account_id, order_type=ORDER_TYPE_MARKET, order_id=order_id)
            response: PostOrderResponse = self.provider.call_function(self.provider.stub_orders.PostOrder, request)
            order.addinfo(order_id=response.order_id)  # Номер заявки добавляем в заявку
        elif order.exectype == Order.Limit:  # Лимитная заявка
            direction = ORDER_DIRECTION_BUY if order.isbuy() else ORDER_DIRECTION_SELL  # Покупка/продажа
            price = self.provider.float_to_quotation(self.provider.price_to_tinkoff_price(class_code, symbol, order.price))  # Лимитная цена
            request = PostOrderRequest(instrument_id=si.figi, quantity=quantity, price=price, direction=direction, account_id=self.account_id, order_type=ORDER_TYPE_LIMIT, order_id=order_id)
            response: PostOrderResponse = self.provider.call_function(self.provider.stub_orders.PostOrder, request)
            order.addinfo(order_id=response.order_id)  # Номер заявки добавляем в заявку
        elif order.exectype == Order.Stop:  # Стоп заявка
            direction = STOP_ORDER_DIRECTION_BUY if order.isbuy() else STOP_ORDER_DIRECTION_SELL  # Покупка/продажа
            price = self.provider.float_to_quotation(self.provider.price_to_tinkoff_price(class_code, symbol, order.price))  # Стоп цена
            request = PostStopOrderRequest(instrument_id=si.figi, quantity=quantity, stop_price=price, direction=direction, account_id=self.account_id,
                                           expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL, stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS)
            response: PostStopOrderResponse = self.provider.call_function(self.provider.stub_stop_orders.PostStopOrder, request)
            order.addinfo(stop_order_id=response.stop_order_id)  # Уникальный идентификатор стоп-заявки добавляем в заявку
        elif order.exectype == Order.StopLimit:  # Стоп-лимитная заявка
            direction = STOP_ORDER_DIRECTION_BUY if order.isbuy() else STOP_ORDER_DIRECTION_SELL  # Покупка/продажа
            price = self.provider.float_to_quotation(self.provider.price_to_tinkoff_price(class_code, symbol, order.price))  # Стоп цена
            pricelimit = self.provider.float_to_quotation(self.provider.price_to_tinkoff_price(class_code, symbol, order.pricelimit))  # Лимитная цена
            request = PostStopOrderRequest(instrument_id=si.figi, quantity=quantity, stop_price=price, price=pricelimit, direction=direction, account_id=self.account_id,
                                           expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL, stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT)
            response: PostStopOrderResponse = self.provider.call_function(self.provider.stub_stop_orders.PostStopOrder, request)
            order.addinfo(stop_order_id=response.stop_order_id)  # Уникальный идентификатор стоп-заявки добавляем в заявку
        order.submit(self)  # Отправляем заявку на биржу (Order.Submitted)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке заявки на биржу
        order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        self.orders[order.ref] = order  # Сохраняем заявку в списке заявок, отправленных на биржу
        return order  # Возвращаем заявку

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        if order.exectype in (Order.Market, Order.Limit):  # Для рыночной и лимитной заявки
            request = CancelOrderRequest(account_id=self.account_id, order_id=order.info['order_id'])  # Отмена активной заявки
            self.provider.call_function(self.provider.stub_orders.CancelOrder, request)
        elif order.exectype in (Order.Stop, Order.StopLimit):  # Для стоп и стоп-лимитной заявки
            request = CancelStopOrderRequest(account_id=self.account_id, stop_order_id=order.info['stop_order_id'])  # Отмена активной стоп заявки
            self.provider.call_function(self.provider.stub_stop_orders.CancelStopOrder, request)
        return order  # В список уведомлений ничего не добавляем. Ждем события on_order

    def oco_pc_check(self, order):
        """
        Проверка связанных заявок
        Проверка родительской/дочерних заявок
        """
        ocos = self.ocos.copy()  # Пока ищем связанные заявки, они могут измениться. Поэтому, работаем с копией
        for order_ref, oco_ref in ocos.items():  # Пробегаемся по списку связанных заявок
            if oco_ref == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in ocos.keys():  # Если у этой заявки указана связанная заявка
            oco_ref = ocos[order.ref]  # то получаем номер транзакции связанной заявки
            self.cancel_order(self.orders[oco_ref])  # отменяем связанную заявку

        if not order.parent and not order.transmit and order.status == Order.Completed:  # Если исполнена родительская заявка
            pcs = self.pcs[order.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent:  # Пропускаем первую (родительскую) заявку
                    self.place_order(child)  # Отправляем дочернюю заявку на биржу
        elif order.parent:  # Если исполнена/отменена дочерняя заявка
            pcs = self.pcs[order.parent.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent and child.ref != order.ref:  # Пропускаем первую (родительскую) заявку и исполненную заявку
                    self.cancel_order(child)  # Отменяем дочернюю заявку

    def on_order_trades(self, event: OrderTrades):
        order: Order = self.get_order(event.order_id)  # Заявка BackTrader
        for trade in event.trades:  # Пробегаемся по всем сделкам заявки
            dt = self.provider.timestamp_to_msk_datetime(trade.date_time)  # Дата и время сделки по времени биржи (МСК)
            pos = self.getposition(order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
            size = trade.quantity  # Количество штук в сделке
            price = trade.price  # Цена за 1 инструмент, по которой совершена сделка
            psize, pprice, opened, closed = pos.update(size, price)  # Обновляем размер/цену позиции на размер/цену сделки
            order.execute(dt, size, price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
            if order.executed.remsize:  # Если осталось что-то к исполнению
                if order.status != order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                    order.partial()  # то заявка частично исполнена
                    self.notifs.append(order.clone())  # Уведомляем брокера о частичном исполнении заявки
            else:  # Если зничего нет к исполнению
                order.completed()  # то заявка полностью исполнена
                self.notifs.append(order.clone())  # Уведомляем брокера о полном исполнении заявки
                # Снимаем oco-заявку только после полного исполнения заявки
                # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
                self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
