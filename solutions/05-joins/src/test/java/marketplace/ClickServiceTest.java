package marketplace;

import org.apache.flink.table.api.*;
import org.apache.flink.table.expressions.Expression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

@Tag("UnitTest")
class ClickServiceTest {
    private ClickService service;
    private TableEnvironment mockEnv;
    private Table mockClicksTable;
    private Table mockOrdersTable;
    private Table mockJoinedTable;
    private TableResult mockResult;
    private TablePipeline mockPipeline;

    @BeforeEach
    public void setup() {
        mockEnv = mock(TableEnvironment.class);
        mockClicksTable = mock(Table.class);
        mockOrdersTable = mock(Table.class);
        mockJoinedTable = mock(Table.class);
        mockPipeline = mock(TablePipeline.class);
        mockResult = mock(TableResult.class);
        service = new ClickService(
            mockEnv,
            "clickTable",
            "orderTable",
            "orderPlacedAfterClickTable"
        );

        when(mockEnv.from("clickTable")).thenReturn(mockClicksTable);
        when(mockClicksTable.select(any(Expression[].class))).thenReturn(mockClicksTable);
        when(mockEnv.from("orderTable")).thenReturn(mockOrdersTable);
        when(mockOrdersTable.select(any(Expression[].class))).thenReturn(mockOrdersTable);
        when(mockClicksTable.join(any())).thenReturn(mockJoinedTable);
        when(mockOrdersTable.join(any())).thenReturn(mockJoinedTable);
        when(mockJoinedTable.where(any())).thenReturn(mockJoinedTable);
        when(mockJoinedTable.select(any(Expression[].class))).thenReturn(mockJoinedTable);
        when(mockJoinedTable.insertInto(anyString())).thenReturn(mockPipeline);
        when(mockPipeline.execute()).thenReturn(mockResult);
        when(mockEnv.executeSql(anyString())).thenReturn(mockResult);
    }

    @Test
    public void createOrderPlacedAfterClickTable_shouldSendTheExpectedSQL() {
        TableResult result = service.createOrderPlacedAfterClickTable();

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);

        verify(mockEnv).executeSql(captor.capture());

        String sql = captor.getValue();

        assertTrue(sql.toLowerCase().contains("create table if not exists"));
        assertTrue(sql.contains("orderPlacedAfterClickTable"));
        assertTrue(sql.contains("customer_id"));
        assertTrue(sql.contains("clicked_url"));
        assertTrue(sql.contains("time_of_click"));
        assertTrue(sql.contains("purchased_product"));
        assertTrue(sql.contains("time_of_order"));
        assertTrue(sql.contains("scan.startup.mode"));
        assertTrue(sql.contains("earliest-offset"));

        assertEquals(mockResult, result);
    }

    @Test
    public void streamOrderPlacedAfterClick_shouldStreamTheExpectedRecordsToTheTable() {
        Duration withinTimePeriod = Duration.ofMinutes(5);
        TableResult result = service.streamOrderPlacedAfterClick(withinTimePeriod);

        verify(mockEnv).from("clickTable");
        verify(mockEnv).from("orderTable");

        ArgumentCaptor<Expression> whereCaptor = ArgumentCaptor.forClass(Expression.class);
        verify(mockJoinedTable).where(whereCaptor.capture());
        String whereExpression = whereCaptor.getValue().asSummaryString();

        assertTrue(whereExpression.contains("and"));
        assertTrue(
            whereExpression.contains("equals(user_id, customer_id")
                || whereExpression.contains("equals(customer_id, user_id)")
        );
        assertTrue(whereExpression.contains(Long.toString(withinTimePeriod.toSeconds())));

        ArgumentCaptor<Expression[]> selectCaptor = ArgumentCaptor.forClass(Expression[].class);
        verify(mockJoinedTable).select(selectCaptor.capture());
        List<String> selectExpressions = Arrays.stream(selectCaptor.getValue())
            .map(Expression::asSummaryString)
            .toList();

        assertTrue(selectExpressions.get(0).contains("customer_id"));
        assertTrue(selectExpressions.get(1).contains("clicked_url"));
        assertTrue(selectExpressions.get(2).contains("time_of_click"));
        assertTrue(selectExpressions.get(3).contains("purchased_product"));
        assertTrue(selectExpressions.get(4).contains("time_of_order"));

        ArgumentCaptor<String> insertCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockJoinedTable).insertInto(insertCaptor.capture());

        assertEquals(
            "orderPlacedAfterClickTable",
            insertCaptor.getValue().replace("`marketplace`", "marketplace")
        );

        assertEquals(mockResult, result);
    }

}