package marketplace;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.expressions.Expression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@Tag("UnitTest")
public class CustomerServiceTest {

    private CustomerService service;
    private TableEnvironment mockEnv;
    private Table mockTable;
    private TableResult mockResult;

    @BeforeEach
    public void setup() {
        mockEnv = mock(TableEnvironment.class);
        mockTable = mock(Table.class);
        mockResult = mock(TableResult.class);
        service = new CustomerService(mockEnv, "customerTable");

        when(mockEnv.from(anyString())).thenReturn(mockTable);
        when(mockTable.select(any(Expression[].class))).thenReturn(mockTable);
        when(mockTable.execute()).thenReturn(mockResult);
    }

    @Test
    public void allCustomers_shouldSelectAllFields() {
        TableResult result = service.allCustomers();

        verify(mockEnv).from("customerTable");

        verify(mockTable).select(ArgumentMatchers.<Expression>argThat(arg->
            arg.asSummaryString().equals("*")
        ));

        assertEquals(mockResult, result);
    }

    @Test
    public void allCustomerAddresses_shouldSelectOnlyTheRelevantFields() {
        TableResult result = service.allCustomerAddresses();

        verify(mockEnv).from("customerTable");

        ArgumentCaptor<Expression[]> selectCaptor = ArgumentCaptor.forClass(Expression[].class);
        verify(mockTable).select(selectCaptor.capture());
        List<String> selectArgs = Arrays.stream(selectCaptor.getValue()).map(exp -> exp.asSummaryString()).toList();
        assertArrayEquals(new String[] {"customer_id", "address", "postcode", "city"}, selectArgs.toArray());

        assertEquals(mockResult, result);
    }
}
