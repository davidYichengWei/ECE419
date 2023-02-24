package shared.messages;

import ecs.ECSNode;

public interface IMetadata {
    public void addNode(ECSNode node);
    public void removeNode(ECSNode node);
    public ECSNode findNode(String key);
}
