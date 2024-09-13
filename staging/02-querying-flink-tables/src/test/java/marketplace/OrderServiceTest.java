package marketplace;

import org.apache.flink.table.api.*;
import org.apache.flink.table.expressions.Expression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.lit;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

@Tag("UnitTest")
public class OrderServiceTest {
    private OrderService service;
    private TableEnvironment mockEnv;
    private Table mockTable;
    private TableResult mockResult;
    private TablePipeline mockPipeline;
    private GroupWindowedTable mockGroupWindowedTable;
    private WindowGroupedTable mockWindowGroupedTable;

    @BeforeEach
    public void setup() {
        mockEnv = mock(TableEnvironment.class);
        mockTable = mock(Table.class);
        mockPipeline = mock(TablePipeline.class);
        mockResult = mock(TableResult.class);
        mockGroupWindowedTable = mock(GroupWindowedTable.class);
        mockWindowGroupedTable = mock(WindowGroupedTable.class);
        service = new OrderService(
            mockEnv,
            "orderTable"
        );

        when(mockEnv.from(anyString())).thenReturn(mockTable);
        when(mockTable.select(any(Expression[].class))).thenReturn(mockTable);
        when(mockTable.window(any(GroupWindow.class))).thenReturn(mockGroupWindowedTable);
        when(mockGroupWindowedTable.groupBy(any(Expression[].class))).thenReturn(mockWindowGroupedTable);
        when(mockWindowGroupedTable.select(any(Expression[].class))).thenReturn(mockTable);
        when(mockTable.where(any())).thenReturn(mockTable);
        when(mockTable.insertInto(anyString())).thenReturn(mockPipeline);
        when(mockTable.execute()).thenReturn(mockResult);
        when(mockPipeline.execute()).thenReturn(mockResult);
        when(mockEnv.executeSql(anyString())).thenReturn(mockResult);
    }

    @Test
    public void ordersOver50Dollars_shouldSelectOrdersWhereThePriceIsGreaterThan50() {
        TableResult result = service.ordersOver50Dollars();

        verify(mockEnv).from("orderTable");

        verify(mockTable).select(ArgumentMatchers.<Expression>argThat(arg->
            arg.asSummaryString().equals("*")
        ));

        verify(mockTable).where(ArgumentMatchers.<Expression>argThat(arg ->
            arg.asSummaryString().equals("greaterThanOrEqual(price, 50)")
        ));

        assertEquals(mockResult, result);
    }

    @Test
    public void pricesWithTax_shouldReturnTheRecordIncludingThePriceWithTax() {
        TableResult result = service.pricesWithTax(BigDecimal.valueOf(1.10));

        verify(mockEnv).from("orderTable");

        ArgumentCaptor<Expression[]> selectCaptor = ArgumentCaptor.forClass(Expression[].class);
        verify(mockTable).select(selectCaptor.capture());
        List<String> selectArgs = Arrays.stream(selectCaptor.getValue()).map(exp -> exp.asSummaryString()).toList();
        assertArrayEquals(new String[] {
            "order_id",
            "as(cast(price, DECIMAL(10, 2)), 'original_price')",
            "as(round(times(cast(price, DECIMAL(10, 2)), 1.1), 2), 'price_with_tax')"
        },
            selectArgs.toArray()
        );

        assertEquals(mockResult, result);
    }
}
