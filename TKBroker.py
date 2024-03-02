from typing import Union  # Объединение типов
import collections
from uuid import uuid4  # Номера заявок должны быть уникальными во времени и пространстве
from threading import Thread
import logging

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

from BackTraderTinkoff import TKStore, TKData

from TinkoffPy.grpc.operations_pb2 import PortfolioRequest, PortfolioResponse  # Портфель
from TinkoffPy.grpc.orders_pb2 import (
    PostOrderRequest, CancelOrderRequest, ORDER_DIRECTION_BUY, ORDER_DIRECTION_SELL, ORDER_TYPE_MARKET, ORDER_TYPE_LIMIT, OrderTrades)  # Заявка
from TinkoffPy.grpc.stoporders_pb2 import (
    PostStopOrderRequest, CancelStopOrderRequest, STOP_ORDER_DIRECTION_BUY, STOP_ORDER_DIRECTION_SELL, StopOrderExpirationType, StopOrderType)  # Стоп-заявка


# noinspection PyArgumentList
class MetaTKBroker(BrokerBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaTKBroker, self).__init__(name, bases, dct)  # Инициализируем класс брокера
        TKStore.BrokerCls = self  # Регистрируем класс брокера в хранилище Tinkoff


# noinspection PyProtectedMember,PyArgumentList,PyUnusedLocal
class TKBroker(with_metaclass(MetaTKBroker, BrokerBase)):
    """Брокер Tinkoff"""
    logger = logging.getLogger('TKBroker')  # Будем вести лог
    currency = PortfolioRequest.CurrencyRequest.RUB  # Суммы будем получать в российских рублях

    def __init__(self, **kwargs):
        super(TKBroker, self).__init__()
        self.store = TKStore(**kwargs)  # Хранилище Tinkoff
        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках
        self.startingcash = self.cash = 0  # Стартовые и текущие свободные средства по счету
        self.startingvalue = self.value = 0  # Стартовая и текущая стоимость позиций
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.ocos = {}  # Список связанных заявок (One Cancel Others)
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)

        self.store.provider.on_order_trades = self.on_order_trades  # Обработка сделок по заявке
        Thread(target=self.store.provider.subscriptions_trades_handler, name='SubscriptionsTradesThread', args=[accounts.id for accounts in self.store.provider.accounts]).start()  # Создаем и запускаем поток обработки подписок сделок по заявке

    def start(self):
        super(TKBroker, self).start()
        self.get_all_active_positions()  # Получаем все активные позиции

    def getcash(self, account=None):
        """Свободные средства по счету, по всем счетам"""
        cash = 0  # Будем набирать свободные средства
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            if account:  # Если считаем свободные средства по счету
                cash = next((position.price for key, position in self.positions.items() if key[0] == account and not key[1]), None)  # Денежная позиция по портфелю/рынку
            else:  # Если считаем свободные средства по всем счетам
                cash = sum([position.price for key, position in self.positions.items() if not key[1]])  # Сумма всех денежных позиций
                self.cash = cash  # Сохраняем текущие свободные средства
        return self.cash

    def getvalue(self, datas=None, account=None):
        """Стоимость позиции, позиций по счету, всех позиций"""
        value = 0  # Будем набирать стоимость позиций
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            if datas:  # Если считаем стоимость позиции/позиций
                data: TKData  # Данные Финам
                for data in datas:  # Пробегаемся по всем тикерам
                    position = self.positions[(data.client_id, data.board, data.symbol)]  # Позиция по тикеру
                    value += position.price * position.size  # Добавляем стоимость позиции по тикеру
            elif account:  # Если считаем свободные средства по счету
                value = sum([position.price * position.size for key, position in self.positions.items() if key[0] == account and key[1]])  # Стоимость позиций по портфелю/бирже
            else:  # Если считаем стоимость всех позиций
                value = sum([position.price * position.size for key, position in self.positions.items() if key[1]])  # Стоимость всех позиций
                self.value = value  # Сохраняем текущую стоимость позиций
        return value

    def getposition(self, data: TKData):
        """Позиция по тикеру
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.positions[(data.account.id, data.class_code, data.symbol)]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

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
        self.store.provider.on_order_trades = self.store.provider.default_handler  # Обработка сделок по заявке
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Функции

    def get_all_active_positions(self):
        """Все активные позиции по счету"""
        cash = 0  # Будем набирать свободные средства
        value = 0  # Будем набирать стоимость позиций
        for account in self.store.provider.accounts:
            request = PortfolioRequest(account_id=account.id, currency=self.currency)  # Запрос портфеля по счету в рублях
            response: PortfolioResponse = self.store.provider.call_function(self.store.provider.stub_operations.GetPortfolio, request)  # Портфель по счету
            cash += self.store.provider.money_value_to_float(response.total_amount_currencies, self.currency)  # Увеличиваем общий размер свободных средств
            for position in response.positions:  # Пробегаемся по всем активным позициям счета
                si = self.store.provider.figi_to_symbol_info(position.figi)  # Поиск тикера по уникальному коду
                size = self.store.provider.quotation_to_float(position.quantity)  # Кол-во в штуках
                price = self.store.provider.money_value_to_float(position.average_position_price)  # Цена входа
                value += price * size  # Увеличиваем общий размер стоимости позиций
                self.positions[(account.id, si.class_code, si.ticker)] = Position(size, price)  # Сохраняем в списке открытых позиций
        self.cash = cash  # Сохраняем текущие свободные средства
        self.value = value  # Сохраняем текущую стоимость позиций

    def get_order(self, order_id: str) -> Union[Order, None]:
        """Заявка BackTrader по номеру заявки на бирже
        Пробегаемся по всем заявкам на бирже. Если нашли совпадение с номером заявки на бирже, то возвращаем заявку BackTrader. Иначе, ничего не найдено

        :param str order_id: Номер заявки на бирже
        :return: Заявка BackTrader или None
        """
        return next((order for order in self.orders.values() if order.info['order_id'] == order_id), None)

    def create_order(self, owner, data: TKData, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, is_buy=True, **kwargs):
        """Создание заявки. Привязка параметров счета и тикера. Обработка связанных и родительской/дочерних заявок
        Даполнительные параметры передаются через **kwargs:
        - account_id - Порядковый номер счета
        """
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, transmit=transmit)  # Заявка на покупку/продажу
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные параметры, в т.ч. account_id
        if order.exectype in (Order.Close, Order.StopTrail, Order.StopTrailLimit, Order.Historical):  # Эти типы заявок не реализованы
            print(f'Постановка заявки {order.ref} по тикеру {data.class_code}.{data.symbol} отклонена. Работа с заявками {order.exectype} не реализована')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        account = self.store.provider.accounts[order.info['account_id']] if 'account_id' in order.info else data.account  # Торговый счет из заявки/тикера
        order.addinfo(account=account.id)  # Сохраняем в заявке
        if order.exectype != Order.Market and not order.price:  # Если цена заявки не указана для всех заявок, кроме рыночной
            price_type = 'Лимитная' if order.exectype == Order.Limit else 'Стоп'  # Для стоп заявок это будет триггерная (стоп) цена
            print(f'Постановка заявки {order.ref} по тикеру {data.class_code}.{data.symbol} отклонена. {price_type} цена (price) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if order.exectype == Order.StopLimit and not order.pricelimit:  # Если лимитная цена не указана для стоп-лимитной заявки
            print(f'Постановка заявки {order.ref} по тикеру {data.class_code}.{data.symbol} отклонена. Лимитная цена (pricelimit) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                print(f'Постановка заявки {order.ref} по тикеру {data.class_code}.{data.symbol} отклонена. Родительская заявка не найдена')
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
        account = order.info['account']  # Торговый счет
        class_code = order.data.class_code  # Код режима торгов
        symbol = order.data.symbol  # Тикер
        si = self.store.provider.get_symbol_info(class_code, symbol)  # Поиск тикера по коду площадки/названию
        quantity: int = abs(order.size // si.lot)  # Размер позиции в лотах. В Тинькофф всегда передается положительный размер лота
        order_id = str(uuid4())  # Уникальный идентификатор заявки
        response = None  # Результат запроса
        if order.exectype == Order.Market:  # Рыночная заявка
            direction = ORDER_DIRECTION_BUY if order.isbuy() else ORDER_DIRECTION_SELL  # Покупка/продажа
            request = PostOrderRequest(instrument_id=si.figi, quantity=quantity, direction=direction, account_id=self.account_id, order_type=ORDER_TYPE_MARKET, order_id=order_id)
            response = self.store.provider.call_function(self.store.provider.stub_orders.PostOrder, request)
        elif order.exectype == Order.Limit:  # Лимитная заявка
            direction = ORDER_DIRECTION_BUY if order.isbuy() else ORDER_DIRECTION_SELL  # Покупка/продажа
            price = self.store.provider.float_to_quotation(self.store.provider.price_to_tinkoff_price(class_code, symbol, order.price))  # Лимитная цена
            request = PostOrderRequest(instrument_id=si.figi, quantity=quantity, price=price, direction=direction, account_id=self.account_id, order_type=ORDER_TYPE_LIMIT, order_id=order_id)
            response = self.store.provider.call_function(self.store.provider.stub_orders.PostOrder, request)
        elif order.exectype == Order.Stop:  # Стоп заявка
            direction = STOP_ORDER_DIRECTION_BUY if order.isbuy() else STOP_ORDER_DIRECTION_SELL  # Покупка/продажа
            price = self.store.provider.float_to_quotation(self.store.provider.price_to_tinkoff_price(class_code, symbol, order.price))  # Стоп цена
            request = PostStopOrderRequest(instrument_id=si.figi, quantity=quantity, stop_price=price, direction=direction, account_id=self.account_id,
                                           expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL, stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS)
            response = self.store.provider.call_function(self.store.provider.stub_stop_orders.PostStopOrder, request)
        elif order.exectype == Order.StopLimit:  # Стоп-лимитная заявка
            direction = STOP_ORDER_DIRECTION_BUY if order.isbuy() else STOP_ORDER_DIRECTION_SELL  # Покупка/продажа
            price = self.store.provider.float_to_quotation(self.store.provider.price_to_tinkoff_price(class_code, symbol, order.price))  # Стоп цена
            pricelimit = self.store.provider.float_to_quotation(self.store.provider.price_to_tinkoff_price(class_code, symbol, order.pricelimit))  # Лимитная цена
            request = PostStopOrderRequest(instrument_id=si.figi, quantity=quantity, stop_price=price, price=pricelimit, direction=direction, account_id=self.account_id,
                                           expiration_type=StopOrderExpirationType.STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL, stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LIMIT)
            response = self.store.provider.call_function(self.store.provider.stub_stop_orders.PostStopOrder, request)
        order.submit(self)  # Отправляем заявку на биржу (Order.Submitted)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке заявки на биржу
        if not response:  # Если при отправке заявки на биржу произошла веб ошибка
            self.logger.warning(f'Постановка заявки по тикеру {class_code}.{symbol} отклонена. Ошибка веб сервиса')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if order.exectype in (Order.Market, Order.Limit):  # Для рыночной и лимитной заявки
            order.addinfo(order_id=response.order_id)  # Номер заявки добавляем в заявку
        elif order.exectype in (Order.Stop, Order.StopLimit):  # Для стоп и стоп-лимитной заявки
            order.addinfo(stop_order_id=response.stop_order_id)  # Уникальный идентификатор стоп-заявки добавляем в заявку
        order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        self.orders[order.ref] = order  # Сохраняем заявку в списке заявок, отправленных на биржу
        return order  # Возвращаем заявку

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        if order.exectype in (Order.Market, Order.Limit):  # Для рыночной и лимитной заявки
            request = CancelOrderRequest(account_id=self.account_id, order_id=order.info['order_id'])  # Отмена активной заявки
            self.store.provider.call_function(self.store.provider.stub_orders.CancelOrder, request)
        elif order.exectype in (Order.Stop, Order.StopLimit):  # Для стоп и стоп-лимитной заявки
            request = CancelStopOrderRequest(account_id=self.account_id, stop_order_id=order.info['stop_order_id'])  # Отмена активной стоп заявки
            self.store.provider.call_function(self.store.provider.stub_stop_orders.CancelStopOrder, request)
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
            dt = self.store.provider.timestamp_to_msk_datetime(trade.date_time)  # Дата и время сделки по времени биржи (МСК)
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
