import torch.nn as nn

class MultiHorizonClassifier(nn.Module):
    def __init__(self, input_dim, hidden_dim=128, num_classes=5):
        super(MultiHorizonClassifier, self).__init__()
        self.shared = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.3)
        )
        self.head1 = nn.Linear(hidden_dim, num_classes)
        self.head6 = nn.Linear(hidden_dim, num_classes)
        self.head12 = nn.Linear(hidden_dim, num_classes)
    
    def forward(self, x):
        x = self.shared(x)
        out1 = self.head1(x)
        out6 = self.head6(x)
        out12 = self.head12(x)
        return out1, out6, out12
