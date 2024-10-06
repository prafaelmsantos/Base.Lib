namespace Base.Lib.Kafka.ServiceRegistration
{
    public static class BrokerExtension
    {
        public static Type[] GetAllMessageTypesFromAssemblies()
        {
            HashSet<Assembly> assembliesToMap = [];
            Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory, "*.dll").ToList().ForEach(
                file => { assembliesToMap.Add(new AssemblyLoader().LoadFromAssemblyPath(file)); });

            HashSet<Type> brokerMessagesToRegister = [];
            foreach (Assembly asm in assembliesToMap)
            {
                List<AssemblyName> refs = [.. asm.GetReferencedAssemblies(), asm.GetName()];
                foreach (AssemblyName refAssembly in refs.Distinct())
                {
                    try
                    {
                        Assembly refLoad = Assembly.Load(refAssembly);
                        IEnumerable<Type> BrokerMessages = refLoad.ExportedTypes
                            .Where(t => typeof(IBaseBrokerMessage).GetTypeInfo().IsAssignableFrom(t.GetTypeInfo()))
                            .Where(t => !t.GetTypeInfo().IsAbstract);
                        if (!BrokerMessages.Any())
                        {
                            continue;
                        }

                        foreach (Type BrokerMessage in BrokerMessages)
                        {
                            brokerMessagesToRegister.Add(BrokerMessage);
                        }
                    }
                    catch (Exception)
                    {
                    }
                }
            }

            return brokerMessagesToRegister.ToArray();
        }

        private class AssemblyLoader : AssemblyLoadContext
        {
            protected override Assembly Load(AssemblyName assemblyName)
            {
                List<CompilationLibrary> res = DependencyContext.Default?.CompileLibraries
                    ?.Where(d => d.Name.Contains(assemblyName.Name ?? string.Empty))
                    ?.ToList() ?? [];

                return Assembly.Load(new AssemblyName(res.First().Name));
            }
        }
    }
}
