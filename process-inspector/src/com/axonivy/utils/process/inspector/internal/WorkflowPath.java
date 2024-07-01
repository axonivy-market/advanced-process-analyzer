package com.axonivy.utils.process.inspector.internal;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;

import com.axonivy.utils.process.inspector.internal.helper.AnalysisPathHelper;
import com.axonivy.utils.process.inspector.internal.model.AnalysisPath;
import com.axonivy.utils.process.inspector.internal.model.CommonElement;
import com.axonivy.utils.process.inspector.internal.model.ProcessElement;
import com.axonivy.utils.process.inspector.internal.model.SubProcessGroup;
import com.axonivy.utils.process.inspector.internal.model.TaskParallelGroup;
import com.axonivy.utils.process.inspector.model.DetectedAlternative;
import com.axonivy.utils.process.inspector.model.DetectedElement;
import com.axonivy.utils.process.inspector.model.DetectedTask;
import com.axonivy.utils.process.inspector.model.ElementTask;

import ch.ivyteam.ivy.process.model.BaseElement;
import ch.ivyteam.ivy.process.model.HierarchicElement;
import ch.ivyteam.ivy.process.model.connector.SequenceFlow;
import ch.ivyteam.ivy.process.model.element.EmbeddedProcessElement;
import ch.ivyteam.ivy.process.model.element.SingleTaskCreator;
import ch.ivyteam.ivy.process.model.element.TaskAndCaseModifier;
import ch.ivyteam.ivy.process.model.element.activity.SubProcessCall;
import ch.ivyteam.ivy.process.model.element.event.end.EmbeddedEnd;
import ch.ivyteam.ivy.process.model.element.event.end.TaskEnd;
import ch.ivyteam.ivy.process.model.element.gateway.Alternative;
import ch.ivyteam.ivy.process.model.element.gateway.TaskSwitchGateway;
import ch.ivyteam.ivy.process.model.element.value.MacroExpression;
import ch.ivyteam.ivy.process.model.element.value.task.TaskConfig;
import ch.ivyteam.ivy.process.model.element.value.task.TaskIdentifier;

class WorkflowPath {
	private static final Duration DURATION_MIN = Duration.ofMillis(Long.MIN_VALUE);
	private ProcessGraph processGraph;
	private boolean isEnableDescribeAlternative;
	private Map<String, String> processFlowOverrides = emptyMap();
	private Map<ElementTask, Duration> durationOverrides = emptyMap();

	protected WorkflowPath() {
		this.processGraph = new ProcessGraph();
	}

	protected WorkflowPath setProcessFlowOverrides(Map<String, String> processFlowOverrides) {
		this.processFlowOverrides = processFlowOverrides;
		return this;
	}

	protected WorkflowPath setDurationOverrides(Map<ElementTask, Duration> durationOverrides) {
		this.durationOverrides = durationOverrides;
		return this;
	}

	protected WorkflowPath setIsEnableDescribeAlternative(boolean isEnableDescribeAlternative) {
		this.isEnableDescribeAlternative = isEnableDescribeAlternative;
		return this;
	}

	protected List<DetectedElement> findAllTasks(Map<ProcessElement, Duration> timeUntilStarts, Enum<?> useCase)
			throws Exception {
		Map<ProcessElement, List<AnalysisPath>> paths = this.pathFinder(timeUntilStarts, null).findAllTask();

		List<DetectedElement> detectedTasks = convertToDetectedElements(paths, useCase, timeUntilStarts);
		return detectedTasks;
	}

	protected List<DetectedElement> findTaskOnPath(Map<ProcessElement, Duration> timeUntilStarts, Enum<?> useCase,
			String flowName) throws Exception {
		Map<ProcessElement, List<AnalysisPath>> paths = this.pathFinder(timeUntilStarts, flowName).findTaskOnPath();

		List<DetectedElement> detectedTasks = convertToDetectedElements(paths, useCase, timeUntilStarts);
		return detectedTasks;
	}

	protected List<DetectedElement> convertToDetectedElements(Map<ProcessElement, List<AnalysisPath>> allPaths, Enum<?> useCase, Map<ProcessElement, Duration> timeUntilStarts) {
		List<DetectedElement> result = new ArrayList<>();
		for (Entry<ProcessElement, List<AnalysisPath>> path : allPaths.entrySet()) {
			List<DetectedElement> elements = convertPathToDetectedElements(path.getKey(), path.getValue(), useCase,
					timeUntilStarts);
			result.addAll(elements);
		}
		result = keepMaxTimeUtilEndDetectedElement(result);
		return result;
	}

	private List<DetectedElement> convertPathToDetectedElements(ProcessElement startElement, List<AnalysisPath> paths,
			Enum<?> useCase, Map<ProcessElement, Duration> timeUntilStarts) {
		List<List<DetectedElement>> allPaths = new ArrayList<>();
		paths.forEach(path -> {
			ProcessElement correctedStartElement = correctStartElement(startElement, path, timeUntilStarts);
			allPaths.add(convertPathToDetectedElements(correctedStartElement, path, useCase, timeUntilStarts));
		});
		List<DetectedElement> result = allPaths.stream().flatMap(List::stream).toList();
		return result;
	}

	private ProcessElement correctStartElement(ProcessElement startElement, AnalysisPath path,
			Map<ProcessElement, Duration> timeUntilStarts) {
		if (timeUntilStarts.containsKey(startElement)) {
			return startElement;
		}

		if (isNotEmpty(path.getElements())) {
			ProcessElement firstElement = path.getElements().get(0);
			if (timeUntilStarts.containsKey(firstElement)) {
				return firstElement;
			}
		}
		// Unknown case
		return startElement;
	}

	private List<DetectedElement> convertPathToDetectedElements(ProcessElement startElement, AnalysisPath path,
			Enum<?> useCase, Map<ProcessElement, Duration> timeUntilStarts) {
		List<DetectedElement> result = new ArrayList<>();
		Duration timeUntilStart = timeUntilStarts.get(startElement);
		Duration durationStart = timeUntilEnd(result, timeUntilStart);

		int lengh = path.getElements().size();

		for (int i = 0; i < lengh; i++) {
			ProcessElement element = path.getElements().get(i); 
			ProcessElement nextElement = i < lengh - 1 ? path.getElements().get(i + 1) : null;

			//It is start node -> Ignored
			if (processGraph.isRequestStart(element.getElement())) {
				continue;
			}

			//It is system task -> Ignored
			if (element instanceof CommonElement 
					&& processGraph.isTaskAndCaseModifier(element.getElement())
					&& processGraph.isSystemTask(element.getElement())) {
				continue;
			}

			//It is alternative -> base on this.isEnableDescribeAlternative to decide
			if (this.isEnableDescribeAlternative && processGraph.isAlternative(element.getElement())) {
				var detectedAlternative = createDetectedAlternative(element);
				if (detectedAlternative != null) {
					result.add(detectedAlternative);
				}
			}

			// Sub process call
			if (processGraph.isSubProcessCall(element.getElement())) {
				SubProcessCall subProcessCall = (SubProcessCall) element.getElement();
				if (processGraph.isHandledAsTask(subProcessCall)) {
					var detectedSubProcessCall = createDetectedTaskFromSubProcessCall(subProcessCall, useCase, durationStart);
					if (detectedSubProcessCall != null) {
						result.add(detectedSubProcessCall);
						durationStart = timeUntilEnd(result, timeUntilStart);
					}
				}
			}

			// // It is User Task - CommonElement(SingleTaskCreator)
			if (processGraph.isSingleTaskCreator(element.getElement())) {
				SingleTaskCreator singleTask = (SingleTaskCreator) element.getElement();
				var detectedTask = createDetectedTask(singleTask, useCase, durationStart);
				if (detectedTask != null) {
					result.add(detectedTask);
					durationStart = timeUntilEnd(result, timeUntilStart);
				}
				continue;
			}

			// It is EmbeddedProcessElement
			if (element instanceof SubProcessGroup) {				
				List<AnalysisPath> internalPaths = ((SubProcessGroup) element).getInternalPaths();
				List<AnalysisPath> subPaths = getAnalysisPathBaseOnNextSequenceFlow(internalPaths, nextElement);
				List<DetectedElement> allTaskFromSubPath = new ArrayList<>();
				for(AnalysisPath subPath : subPaths) {
					ProcessElement startSubElement = subPath.getElements().get(0);
					var startedForSubProcess = new HashedMap<>(timeUntilStarts);
					startedForSubProcess.put(startSubElement, durationStart);
							
					 List<DetectedElement> subResult = convertPathToDetectedElements(startSubElement, subPath, useCase , startedForSubProcess);
					 allTaskFromSubPath.addAll(subResult);
				}
				
				if(isNotEmpty(allTaskFromSubPath)) {
					result.addAll(allTaskFromSubPath);
					durationStart = getMaxDurationUntilEnd(allTaskFromSubPath);
				}
				
				continue;
			}
			
			if (element instanceof TaskParallelGroup) {
				var startedForGroup = element.getElement() == null ? timeUntilStarts : Map.of(element, durationStart);

				TaskParallelGroup group = (TaskParallelGroup) element;
				var tasks = convertTaskParallelGroupToDetectedElement(group, useCase, startedForGroup);
				if (isNotEmpty(tasks)) {
					result.addAll(tasks);
					durationStart = getMaxDurationUntilEnd(tasks);
				}
				continue;
			}

			if (element instanceof CommonElement && processGraph.isSequenceFlow(element.getElement())) {
				SequenceFlow sequenceFlow = (SequenceFlow) element.getElement();
				if (sequenceFlow.getSource() instanceof TaskSwitchGateway) {
					var startTask = createStartTaskFromTaskSwitchGateway(sequenceFlow, durationStart, useCase);
					if (startTask != null) {
						result.add(startTask);
						durationStart = timeUntilEnd(result, timeUntilStart);
					}
					continue;
				}
			}
		}
		return result;
	}

	private List<DetectedElement> convertTaskParallelGroupToDetectedElement(TaskParallelGroup group, Enum<?> useCase,
			Map<ProcessElement, Duration> timeUntilStartAts) {
		List<DetectedElement> tasksWithTaskEnd = convertTaskParallelGroupToDetectedElement(group, useCase,
				timeUntilStartAts, true);
		List<DetectedElement> tasksInternal = convertTaskParallelGroupToDetectedElement(group, useCase,
				timeUntilStartAts, false);
		return ListUtils.union(tasksWithTaskEnd, tasksInternal);
	}

	private List<DetectedElement> convertTaskParallelGroupToDetectedElement(TaskParallelGroup group, Enum<?> useCase,
			Map<ProcessElement, Duration> timeUntilStartAts, boolean withTaskEnd) {
		Map<SequenceFlow, List<AnalysisPath>> internalPath = getInternalPath(group.getInternalPaths(), withTaskEnd);

		List<DetectedElement> result = new ArrayList<>();
		for (Entry<SequenceFlow, List<AnalysisPath>> entry : internalPath.entrySet()) {
			SequenceFlow sequenceFlow = entry.getKey();
			Duration startedAt = timeUntilStartAts.get(group);

			ProcessElement key = new CommonElement(sequenceFlow);
			Map<ProcessElement, List<AnalysisPath>> path = Map.of(key, entry.getValue());
			Map<ProcessElement, Duration> startTimeDuration = null;

			List<DetectedElement> tasks = emptyList();
			if (group.getElement() != null) {
				var startTask = createStartTaskFromTaskSwitchGateway(sequenceFlow, startedAt, useCase);
				if (startTask != null) {
					result.add(startTask);
					startedAt = ((DetectedTask) startTask).getTimeUntilEnd();
				}

				startTimeDuration = Map.of(key, startedAt);

			} else {
				startTimeDuration = timeUntilStartAts;
			}

			tasks = convertToDetectedElements(path, useCase, startTimeDuration);
			result.addAll(tasks);
		}

		return result;
	}

	private Duration timeUntilEnd(List<DetectedElement> detectedElements, Duration defaultAt) {
		List<DetectedTask> detectedTasks = detectedElements.stream().filter(item -> item instanceof DetectedTask)
				.map(DetectedTask.class::cast).toList();
		int size = detectedTasks.size();
		Duration duration = defaultAt;
		if (size > 0) {
			duration = detectedTasks.get(size - 1).getTimeUntilEnd();
			if (duration.isNegative()) {
				duration = Duration.ZERO;
			}
		}

		return duration;
	}

	private Duration getMaxDurationUntilEnd(List<DetectedElement> detectedElements) {
		Duration maxDurationUntilEnd = detectedElements.stream()
				.filter(item -> item instanceof DetectedTask == true)
				.map(DetectedTask.class::cast)
				.map(DetectedTask::getTimeUntilEnd)
				.max(Duration::compareTo)
				.orElse(null);

		if (maxDurationUntilEnd == null || maxDurationUntilEnd.isNegative()) {
			maxDurationUntilEnd = Duration.ZERO;
		}
		
		return maxDurationUntilEnd;
	}

	private DetectedElement createStartTaskFromTaskSwitchGateway(SequenceFlow sequenceFlow, Duration timeUntilStartedAt,
			Enum<?> useCase) {
		DetectedElement task = null;
		if (sequenceFlow.getSource() instanceof TaskSwitchGateway) {
			TaskSwitchGateway taskSwitchGateway = (TaskSwitchGateway) sequenceFlow.getSource();
			if (!processGraph.isSystemTask(taskSwitchGateway)) {
				TaskConfig startTask = processGraph.getStartTaskConfig(sequenceFlow);
				task = createDetectedTask(taskSwitchGateway, startTask, useCase, timeUntilStartedAt);
			}
		}
		return task;
	}

	private DetectedAlternative createDetectedAlternative(ProcessElement processElement) {
		if (processGraph.isAlternative(processElement.getElement())) {
			Alternative alternative = (Alternative) processElement.getElement();
			List<DetectedElement> options = alternative.getOutgoing().stream()
					.map(item -> convertToDetectedElement(item)).toList();
			String elementName = alternative.getName();
			String pid = alternative.getPid().getRawPid();
			DetectedAlternative result = new DetectedAlternative(pid, elementName, options);
			return result;
		}
		return null;
	}

	private DetectedElement createDetectedTaskFromSubProcessCall(SubProcessCall subProcessCall, Enum<?> useCase,
			Duration timeUntilStartAt) {
		String pid = subProcessCall.getPid().getRawPid();
		String elementName = subProcessCall.getName();

		String script = subProcessCall.getParameters().getCode();
		String taskName = getTaskNameByCode(script);
		String customerInfo = getCustomInfoByCode(script);
		Duration duration = this.workflowDuration().getDuration(ElementTask.createSingle(pid), script, useCase);
		List<String> parentElementNames = getParentElementNames(subProcessCall);
		
		DetectedTask detectedTask = new DetectedTask(pid, taskName, elementName, timeUntilStartAt, duration,
				parentElementNames, customerInfo);
		return detectedTask;
	}

	private DetectedElement convertToDetectedElement(SequenceFlow outcome) {
		String pid = outcome.getPid().getRawPid();
		String elementName = outcome.getName();

		DetectedElement element = new DetectedElement(pid, elementName);
		return element;
	}

	private DetectedElement createDetectedTask(SingleTaskCreator task, Enum<?> useCase, Duration timeUntilStartAt) {
		return createDetectedTask(task, task.getTaskConfig(), useCase, timeUntilStartAt);
	}

	private DetectedElement createDetectedTask(TaskAndCaseModifier task, TaskConfig taskConfig, Enum<?> useCase,
			Duration timeUntilStartAt) {
		String elementName = task.getName();
		String taskName = getTaskName(taskConfig);
		String script = Optional.ofNullable(taskConfig).map(TaskConfig::getScript).orElse(EMPTY);
		String customerInfo = getCustomInfoByCode(script);

		ElementTask elementTask = processGraph.createElementTask(task, taskConfig);
		Duration duration = this.workflowDuration().getDuration(elementTask, script, useCase);
		List<String> parentElementNames = getParentElementNames(task);

		DetectedTask detectedTask = new DetectedTask(elementTask.getId(), taskName, elementName, timeUntilStartAt,
				duration, parentElementNames, customerInfo);
		return detectedTask;
	}

	private List<DetectedElement> keepMaxTimeUtilEndDetectedElement(List<DetectedElement> detectedElements) {
		Map<String, Duration> taskGroupWithMaxTimeUntilEnd = detectedElements.stream()
				.filter(DetectedTask.class::isInstance).map(DetectedTask.class::cast)
				// Only get greater than value if it is duplicated
				.collect(toMap(DetectedTask::getPid, DetectedTask::getTimeUntilEnd,
						(a, b) -> b.compareTo(a) > 0 ? b : a));

		List<DetectedElement> result = new ArrayList<>();
		for (DetectedElement element : detectedElements) {
			if (isNotContains(result, element)) {
				if (element instanceof DetectedAlternative) {
					result.add(element);
				}

				if (element instanceof DetectedTask) {
					Duration maxDuration = taskGroupWithMaxTimeUntilEnd.getOrDefault(element.getPid(), DURATION_MIN);
					Duration duration = ((DetectedTask) element).getTimeUntilEnd();
					if (duration.compareTo(maxDuration) >= 0) {
						result.add(element);
					}
				}
			}
		}

		return result;
	}

	private List<String> getParentElementNames(HierarchicElement task) {
		List<String> parentElementNames = emptyList();
		if(task instanceof TaskAndCaseModifier || task instanceof SubProcessCall)
		if (task.getParent() instanceof EmbeddedProcessElement) {
			parentElementNames = processGraph.getParentElementNamesEmbeddedProcessElement(task.getParent());
		}
		return parentElementNames;
	}

	private String getCustomInfoByCode(String script) {
		String wfEstimateCode = processGraph.getCodeLineByPrefix(script, "APAConfig.setCustomInfo");
		String result = Optional.ofNullable(wfEstimateCode).filter(StringUtils::isNotEmpty)
				.map(it -> StringUtils.substringBetween(it, "(\"", "\")")).orElse(null);

		return result;
	}

	private String getTaskNameByCode(String script) {
		String wfEstimateCode = processGraph.getCodeLineByPrefix(script, "APAConfig.setTaskName");
		String result = Optional.ofNullable(wfEstimateCode).filter(StringUtils::isNotEmpty)
				.map(it -> StringUtils.substringBetween(it, "(\"", "\")")).orElse(null);

		return result;
	}

	private boolean isNotContains(List<DetectedElement> detectedElements, DetectedElement detectedElement) {
		List<String> pids = detectedElements.stream().map(DetectedElement::getPid).toList();
		return !pids.contains(detectedElement.getPid());
	}

	private Map<SequenceFlow, List<AnalysisPath>> getInternalPath(Map<SequenceFlow, List<AnalysisPath>> internalPath,
			boolean withTaskEnd) {
		Map<SequenceFlow, List<AnalysisPath>> paths = new LinkedHashMap<>();

		// Priority the path go to end first
		for (SequenceFlow sf : internalPath.keySet()) {
			List<AnalysisPath> analysisPaths = new ArrayList<>();
			for (AnalysisPath path : internalPath.get(sf)) {
				ProcessElement last = getLast(path.getElements());
				if (withTaskEnd && last.getElement() instanceof TaskEnd) {
					analysisPaths.add(path);
				} else if (!withTaskEnd && last.getElement() instanceof TaskEnd == false) {
					analysisPaths.add(path);
				}
			}
			if (isNotEmpty(analysisPaths)) {
				paths.put(sf, analysisPaths);
			}
		}

		return paths;
	}

	private <T> T getLast(List<T> elements) {
		return elements.stream().reduce((first, second) -> second).orElse(null);
	}

	private PathFinder pathFinder(Map<ProcessElement, Duration> startAtElements, String flowName) {
		return new PathFinder().setProcessFlowOverrides(processFlowOverrides).setFlowName(flowName)
				.setStartElements(startAtElements.keySet().stream().toList());
	}

	private WorkflowDuration workflowDuration() {
		return new WorkflowDuration().setDurationOverrides(durationOverrides);
	}

	private String getTaskName(TaskConfig taskConfig) {
		String taskNameFromRawMacro = Optional.ofNullable(taskConfig).map(TaskConfig::getName)
				.map(MacroExpression::getRawMacro).orElse(EMPTY);

		String taskIdentifier = Optional.ofNullable(taskConfig).map(TaskConfig::getTaskIdentifier)
				.map(TaskIdentifier::getRawIdentifier).orElse(EMPTY);

		return defaultIfBlank(taskNameFromRawMacro, taskIdentifier);
	}
	
	private List<AnalysisPath> getAnalysisPathBaseOnNextSequenceFlow(List<AnalysisPath> subPaths, ProcessElement nextElement) {		
		if(nextElement == null) {
			return subPaths;
		}
		
		List<AnalysisPath> result = new ArrayList<>();
		BaseElement sequenceFlow = nextElement.getElement();
		if (sequenceFlow instanceof SequenceFlow) {
			for (AnalysisPath path : subPaths) {				
				ProcessElement lastProcessElement = AnalysisPathHelper.getLastElement(path);
				BaseElement lastElement = lastProcessElement.getElement();

				if (processGraph.isTaskEnd(lastElement)) {
					result.add(path);
					continue;
				}

				if (processGraph.isTaskSwitchGateway(lastElement)) {
					Map<SequenceFlow, List<AnalysisPath>> validInternalPaths = new LinkedHashMap<>();
					((TaskParallelGroup) lastProcessElement).getInternalPaths().forEach((key, value) -> {
						var validPaths = getAnalysisPathBaseOnNextSequenceFlow(value, nextElement);
						if (isNotEmpty(validPaths)) {
							validInternalPaths.put(key, validPaths);
						}
					});

					int size = path.getElements().size();
					List<ProcessElement> newPath = path.getElements().stream().limit(size - 1).collect(Collectors.toList());
					if (MapUtils.isNotEmpty(validInternalPaths)) {
						TaskParallelGroup taskGroup = new TaskParallelGroup(lastElement);
						taskGroup.setInternalPaths(validInternalPaths);
						newPath.add(taskGroup);	
					}
					result.add(new AnalysisPath(newPath));
					continue;
				}
				
				if (processGraph.isEmbeddedEnd(lastElement)) {
					BaseElement outerSequenceFlow = ((EmbeddedEnd) lastElement).getConnectedOuterSequenceFlow();
					if(outerSequenceFlow.getPid().equals(sequenceFlow.getPid())) {
						result.add(path);
					}
					continue;
				}

			}
		}
		return result;
	}
}
 