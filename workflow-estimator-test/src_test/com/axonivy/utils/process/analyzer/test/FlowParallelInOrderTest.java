package com.axonivy.utils.process.analyzer.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.axonivy.utils.process.analyzer.AdvancedProcessAnalyzer;
import com.axonivy.utils.process.analyzer.constant.UseCase;
import ch.ivyteam.ivy.environment.IvyTest;

@IvyTest
@SuppressWarnings("restriction")
public class FlowParallelInOrderTest extends FlowExampleTest {
		
	private static final String PROCESS_NAME = "FlowParallelInOrder";
	
	@BeforeAll
	public static void setup() {
		setup(PROCESS_NAME);
	}
	
	@Test
	void shouldFindAllTasksAtStart() throws Exception {
		var processAnalyzer = new AdvancedProcessAnalyzer(process, null, null);
		var start = ProcessGraphHelper.findByElementName(process, "start");
		var detectedTasks = processAnalyzer.findAllTasks(start);

		var expected = Arrays.array("Task 1A", "Task A", "Task B", "Task 1B",  "Task C", "Task D");
		var taskNames = getTaskNames(detectedTasks);
		assertArrayEquals(expected, taskNames);
	}
	
	@Test
	void shouldFindAllTasksAtStart2() throws Exception {
		var processAnalyzer = new AdvancedProcessAnalyzer(process, null, null);
		var start2 = ProcessGraphHelper.findByElementName(process, "start2");
		var detectedTasks = processAnalyzer.findAllTasks(start2);
		
		var expected = Arrays.array("Task 2B", "Task G", "Task K", "Task M", "Task 2A", "Task F", "Task H", "Task 2C", "Task E", "Task I");
		var taskNames = getTaskNames(detectedTasks);
		assertArrayEquals(expected, taskNames);
	}

	@Test
	void shouldFindAllTasksAtStart3() throws Exception {
		var processAnalyzer = new AdvancedProcessAnalyzer(process, null, null);
		var start3 = ProcessGraphHelper.findByElementName(process, "start3");
		var detectedTasks = processAnalyzer.findAllTasks(start3);		
		
		var expected = Arrays.array("Task1A", "Task B", "Task1B", "Task A", "Task2B", "Task D", "Task2A", "Task C", "Task F", "Task K", "Task2C", "Task E", "Task3A", "Task I");
		var taskNames = getTaskNames(detectedTasks);
		assertArrayEquals(expected, taskNames);
	}
	
	@Test
	void shouldFindTasksOnPathAtStart3() throws Exception {
		var processAnalyzer = new AdvancedProcessAnalyzer(process, null, "internal");
		var start3 = ProcessGraphHelper.findByElementName(process, "start3");
		var detectedTasks = processAnalyzer.findTasksOnPath(start3);		
		
		var expected = Arrays.array("Task1A", "Task B", "Task1B", "Task A", "Task2B", "Task D", "Task2A", "Task C", "Task K", "Task2C", "Task3A", "Task I");
		var taskNames = getTaskNames(detectedTasks);
		assertArrayEquals(expected, taskNames);
	}
	
	@Test
	void shouldCalculateTotalDurationWithSMALPROJECT() throws Exception {
		var processAnalyzer = new AdvancedProcessAnalyzer(process, UseCase.SMALLPROJECT, null);
		var start2 = ProcessGraphHelper.findByElementName(process, "start2");
		Duration duration = processAnalyzer.calculateEstimatedDuration(start2);
		assertEquals(10, duration.toHours());
	}
	
	@Test
	void shouldCalculateTotalDurationWithBIGPROJECT() throws Exception {
		var processAnalyzer = new AdvancedProcessAnalyzer(process, UseCase.BIGPROJECT, null);
		var start2 = ProcessGraphHelper.findByElementName(process, "start2");
		Duration duration = processAnalyzer.calculateEstimatedDuration(start2);
		assertEquals(15, duration.toHours());
	}
}