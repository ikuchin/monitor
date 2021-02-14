MetricTemplates - entity that should unite monitors and processors.
 - Specify what monitor to use.
 - What what processor to use.
 - When processor processing a messages it run it through MetricInstances that belong to Job.
 
MetricInstance - Instance of MetricTemplate that belong to a specific Job.
 
Monitors and Processors should be delivered together.
- Processor will depend on monitor.
- Monitor will depend on processor.
- System should report that both are present.