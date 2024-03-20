package com.axonivy.utils.estimator;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;

import com.axonivy.utils.estimator.constant.UseCase;
import com.google.common.collect.Lists;

import ch.ivyteam.ivy.environment.Ivy;
import ch.ivyteam.ivy.process.model.BaseElement;
import ch.ivyteam.ivy.process.model.NodeElement;
import ch.ivyteam.ivy.process.model.connector.SequenceFlow;
import ch.ivyteam.ivy.process.model.element.EmbeddedProcessElement;
import ch.ivyteam.ivy.process.model.element.TaskAndCaseModifier;
import ch.ivyteam.ivy.process.model.element.event.start.RequestStart;
import ch.ivyteam.ivy.process.model.element.gateway.Alternative;
import ch.ivyteam.ivy.process.model.element.gateway.TaskSwitchGateway;
import ch.ivyteam.ivy.process.model.element.value.task.TaskConfig;

@SuppressWarnings("restriction")
abstract class AbstractWorkflow {

	private enum FindType {
		ALL_TASKS, TASKS_ON_PATH
	};

	private ProcessGraph processGraph;

	protected AbstractWorkflow() {
		this.processGraph = new ProcessGraph();
	}

	protected abstract Map<String, Duration> getDurationOverrides();
	protected abstract Map<String, String> getProcessFlowOverrides();

	protected List<BaseElement> findPath(BaseElement... from) throws Exception {
		List<BaseElement> path = findPath(Arrays.asList(from), null, FindType.ALL_TASKS,  emptyList());		
		return path;
	}
	
	protected List<BaseElement> findPath(String flowName, BaseElement... from) throws Exception {
		List<BaseElement> path = findPath(Arrays.asList(from), flowName, FindType.TASKS_ON_PATH, emptyList());
		return path;
	}
	
	protected String getCustomInfoByCode(TaskConfig task) {
		String wfEstimateCode = processGraph.getCodeLineByPrefix(task, "WfEstimate.setCustomInfo");		
		String result = Optional.ofNullable(wfEstimateCode)
				.filter(StringUtils::isNotEmpty)
				.map(it -> StringUtils.substringBetween(it, "(\"", "\")"))
				.orElse(null);;
		
		return result;
	}
	
	protected String getTaskId(TaskAndCaseModifier task, TaskConfig taskConfig) {
		String id = task.getPid().getRawPid();
		if (task instanceof TaskSwitchGateway) {
			return id + "-" + taskConfig.getTaskIdentifier().getRawIdentifier();
		} else {
			return id;
		}
	}
	
	protected Duration getDuration(TaskAndCaseModifier task, TaskConfig taskConfig, UseCase useCase) {
		String key = getTaskId(task, taskConfig);		
		return getDurationOverrides().getOrDefault(key, getDurationByTaskScript(taskConfig, useCase));	
	}
	
	protected boolean isSystemTask(TaskAndCaseModifier task) {		
		return task.getAllTaskConfigs().stream().anyMatch(it -> "SYSTEM".equals(it.getActivator().getName()));
	}
	
	protected List<String> getParentElementNames(TaskAndCaseModifier task){
		List<String> parentElementNames = emptyList();
		if(task.getParent() instanceof EmbeddedProcessElement) {
			parentElementNames = processGraph.getParentElementNamesEmbeddedProcessElement(task.getParent());
		}		
		return parentElementNames ;
	}
	
	private List<BaseElement> findPath(List<BaseElement> froms, String flowName, FindType findType, List<BaseElement> previousElements) throws Exception {
		List<BaseElement> result = new ArrayList<>();
		for(BaseElement from : froms) {
			var path = findPath(from, flowName, findType, emptyList());
			result.addAll(path);
		}
				
		return result.stream().distinct().toList();
	}
	
	/**
	 * Using Recursion Algorithm To Find Tasks On Graph.
	 */
	private List<BaseElement> findPath(BaseElement from, String flowName, FindType findType, List<BaseElement> previousElements) throws Exception {
		// Prevent loop
		if (previousElements.indexOf(from) >= 0) {
			return emptyList();
		}

		List<BaseElement> path = new ArrayList<>();
		path.add(from);

		if (from instanceof NodeElement) {
			if(from instanceof EmbeddedProcessElement) {
				List<BaseElement> pathFromSubProcess = findPathOfSubProcess((EmbeddedProcessElement) from, flowName, findType);
				path.addAll(pathFromSubProcess);
			}			

			if(endTaskSwitchGateway(from)) {
				return path;
			}
						
			if(startTaskSwitchGateway(from)) {
				List<BaseElement> pathFromParallel = getListElementInParallelTask((TaskSwitchGateway) from, flowName, findType, previousElements);
				path.addAll(pathFromParallel);
				
				from = findEndTaskSwithGateWay(from, pathFromParallel);
				if( from == null) {
					return  path;
				}				
			}			
			
			List<SequenceFlow> outs = getSequenceFlows((NodeElement) from, flowName, findType);
			if (from instanceof Alternative && outs.isEmpty()) {
				Ivy.log().error("Can not found the out going from a alternative {0}", from.getPid().getRawPid());
				throw new Exception("Not found path");
			}
			
			Map<SequenceFlow, List<BaseElement>> paths = new HashedMap<>();
			for (SequenceFlow out : outs) {
				List<BaseElement> currentPath = ListUtils.union(previousElements, Arrays.asList(from));
				List<BaseElement> nextOfPath = findPath(out.getTarget(), flowName, findType, currentPath);
				paths.put(out, nextOfPath);
			}

			path.addAll(getPathWithLongestFirst(paths));
		}
		
		return path;
	}
	
	private List<BaseElement> getListElementInParallelTask(TaskSwitchGateway from, String flowName, FindType findType, List<BaseElement> previousElements) throws Exception {
		List<BaseElement> result = new ArrayList<>();
		List<SequenceFlow> outs = getSequenceFlows((NodeElement) from, flowName, findType);
		
		Map<SequenceFlow, List<BaseElement>> paths = new HashedMap<>();
		for (SequenceFlow out : outs) {
			List<BaseElement> currentPath = ListUtils.union(previousElements, Arrays.asList(from));
			List<BaseElement> nextOfPath = findPath(out.getTarget(), flowName, findType, currentPath);
			paths.put(out, nextOfPath);
		}

		result.addAll(getPathWithLongestFirst(paths));
		
		return result;
	}
	
	/**
	 * Find path on sub process
	 */	
	private List<BaseElement> findPathOfSubProcess(EmbeddedProcessElement subProcessElement, String flowName, FindType findType) throws Exception {
		// find start element
		BaseElement start = processGraph.findOneStartElementOfProcess(subProcessElement.getEmbeddedProcess());
		List<BaseElement> path = findPath(start, flowName, findType, emptyList());		
		return path;
	}	
	
	private List<SequenceFlow> getSequenceFlows(NodeElement from, String flowName, FindType findType) {
		if (findType == FindType.ALL_TASKS || from instanceof TaskSwitchGateway && from != null) {
			return from.getOutgoing();
		} else {
			Optional<SequenceFlow> flow = Optional.empty();
			
			//Always is priority check flow from flowOverrides first.
			if(from instanceof Alternative) {
				String flowIdFromOrverride = getProcessFlowOverrides().get(from.getPid().getRawPid());
				flow = from.getOutgoing().stream().filter(out -> out.getPid().getRawPid().equals(flowIdFromOrverride)).findFirst();				
			}
			
			//If it don't find out the flow from flowOverrides, it is base on the default flow in process
			if(flow.isEmpty()) {
				flow = getSequenceFlow(from, flowName);
			}
			
			return flow.map(Arrays::asList).orElse(emptyList());
		}
	}
	
	private Optional<SequenceFlow> getSequenceFlow(NodeElement nodeElement, String flowName) {
		List<SequenceFlow> outs = nodeElement.getOutgoing();
		if(CollectionUtils.isEmpty(outs)) {
			return Optional.empty();
		}
			
		if(nodeElement instanceof Alternative) {
			// High priority for checking default path if flowName is null
			if(isEmpty(flowName)) {
				return outs.stream().filter(out -> isDefaultPath(out)).findFirst();
			} else {
				//If flowName is not null, check flowName first
				Optional<SequenceFlow> flow = outs.stream().filter(out -> hasFlowName(out, flowName)).findFirst();
				// Then check default path
				if(!flow.isPresent()) {
					flow = outs.stream().filter(out -> isDefaultPath(out)).findFirst();
				}
				return flow;
			}
		}
		
		Optional<SequenceFlow> flow = outs.stream()
				.filter(out -> hasFlowNameOrEmpty(out, flowName))
				.findFirst();
			
		return flow;
	}
	
	private boolean hasFlowNameOrEmpty(SequenceFlow sequenceFlow, String flowName) {		
		if(isEmpty(flowName)) {
			return true;
		}
		
		if(hasFlowName(sequenceFlow, flowName)) {
			return true;
		}
		
		if(isEmpty(sequenceFlow.getEdge().getLabel().getText())) {
			return true;
		};

		return false;
	}

	private boolean hasFlowName(SequenceFlow sequenceFlow, String flowName) {
		String label = sequenceFlow.getEdge().getLabel().getText();
		return isNotBlank(label) && label.contains(flowName);
	}
	
	private boolean isDefaultPath(SequenceFlow flow) {
		NodeElement sourceElement = flow.getSource();
		if(sourceElement instanceof Alternative) {
			return isDefaultPath((Alternative) sourceElement, flow);
		}
		
		return false;		
	}
	
	private boolean isDefaultPath(Alternative alternative, SequenceFlow sequenceFlow) {
		String currentElementId = sequenceFlow.getPid().getFieldId();
		String nextTargetId = processGraph.getNextTargetIdByCondition(alternative, EMPTY);
		return Objects.equals(currentElementId, nextTargetId);
	}
	
	private long countNumberAcceptedTasks(List<BaseElement> listElements) {
		return listElements.stream().filter(item -> isAcceptedTask(item)).count();
	}
	
	private boolean isAcceptedTask(BaseElement element) {
		return Optional.ofNullable(element)
				// filter to get task only
				.filter(node -> node instanceof TaskAndCaseModifier)
				.map(TaskAndCaseModifier.class::cast)
				.filter(node -> node instanceof RequestStart == false)
				// Remove SYSTEM task
				.filter(node -> isSystemTask(node) == false)
				.isPresent();
	}	
	
	private Duration getDurationByTaskScript(TaskConfig task, UseCase useCase) {
		List<String> prefixs = new ArrayList<String>(Arrays.asList("WfEstimate.setEstimate"));
		if(useCase != null) {
			prefixs.add("UseCase." + useCase.name());
		}

		String wfEstimateCode = processGraph.getCodeLineByPrefix(task, prefixs.toArray(new String[0]));
		if (isNotEmpty(wfEstimateCode)) {
			String result = StringUtils.substringBetween(wfEstimateCode, "(", "UseCase");
			int amount = Integer.parseInt(result.substring(0, result.indexOf(",")));
			String unit = result.substring(result.indexOf(".") + 1, result.lastIndexOf(","));

			switch (TimeUnit.valueOf(unit.toUpperCase())) {
				case DAYS:
	                return Duration.ofDays(amount);
	            case HOURS:
	                return Duration.ofHours(amount);
	            case MINUTES:
	                return Duration.ofMinutes(amount);
	            case SECONDS:
	                return Duration.ofSeconds(amount);
	            default:
	                // Handle any unexpected TimeUnit
	                break;
			}		
		}

		return Duration.ofHours(0);
	}
	
	private boolean startTaskSwitchGateway(BaseElement task) {
		return task instanceof TaskSwitchGateway &&  !isSystemTask((TaskAndCaseModifier) task);
	}
	
	private boolean endTaskSwitchGateway(BaseElement task) {
		return task instanceof TaskSwitchGateway &&  isSystemTask((TaskAndCaseModifier) task);
	}
	
	private BaseElement findEndTaskSwithGateWay(BaseElement task, List<BaseElement> elements) {
		return (BaseElement) Lists.reverse(elements).stream().filter(item -> endTaskSwitchGateway(item)).findFirst().orElse(null);  
	}
	
	private List<BaseElement> getPathWithLongestFirst(Map<SequenceFlow, List<BaseElement>> paths) {
		List<BaseElement> result = new ArrayList<>();
		paths.entrySet().stream()
				// Consider to count all element of remove this sort (what flow is drawn first
				// it will go first
				.sorted(Map.Entry.comparingByValue(
						Comparator.comparing(it -> countNumberAcceptedTasks(it), Comparator.reverseOrder())))
				.forEach(entry -> {
					result.add(entry.getKey());
					result.addAll(entry.getValue());
				});
		
		return result;
	}
}
